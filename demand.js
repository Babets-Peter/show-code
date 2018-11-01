const redis = require('../../redis');
const { extra } = require('../../config');
const url = require('url');
const CountryTargetingLib = require('../libs/countries');
const BlackWhiteListsLib = require('../libs/bw_list');
const BudgetLimitLib = require('../libs/budgetLimit');
const DeviceTargetingLib = require('../libs/deviceTargeting');
const PlayerSizeTargetingLib = require('../libs/playerSizeTargeting');
const DemandsLib = require('../libs/demands');
const ErrorHandler = require('../libs/errorHandler');
const VastParser = require('../libs/parseVast');
const _ = require('lodash');
const moment = require('moment');
const ReferersBySupplyLib = require('../libs/domains_limit');
const IPCounter = require('../libs/ip_counter');
const fs = require('fs');
const device = require('device');
const collector = require('../libs/collector');

const DEMANDS_IN_QUEUE_PER_REQUEST = 5;
const MAX_REFERERS_FOR_SUPPLY_PER_DAY = 10000;

function returnEmptyVast(res){
    res.writeHead(200, {'Content-Type': 'text/xml'} );
    return res.end( '<VAST version="2.0"></VAST>' );
}

function returnXML(res, xml){
    res.writeHead(200, {'Content-Type': 'text/xml'});
    return res.end(xml);
}

function isIpValid(ip){
    return /^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/.test(ip);
}

function writeEventToRedis(eventName, params, demandId){
    const now = moment();
    const minutes = _.padStart((now.minutes() - now.minutes() % 5).toString(),2,'0');
    const redisKey = `${demandId}:${params.referer}:${params.supplyId}:${params.device.type}:${params.partnerId}:${eventName}:${params.countryCode}:${now.format('YYYY.MM.DD.HH')}.${minutes}`;

    collector({
        supplyId: params.supplyId,
        demandId: demandId,
        referer: params.referer,
        event: eventName,
        device: params.device.type,
        partnerId: params.partnerId,
        countryCode: params.countryCode,
        date: now.format('YYYY.MM.DD.HH')
    }).catch(err => console.error(err, 'Vast Demands - Collector'));


    if (params.ip_counter === true) {
        let args = {
          supply: params.supplyId,
          event: eventName,
          ip: params.ip,
          date: now.format('YYYY.MM.DD.HH')
        };

        try {
          IPCounter(args);
        } catch (e) {
          console.error(e, args);
        }
    }

    return Promise.all([
        redis.zincrbyAsync('tracking_events', 1, redisKey),
        redis.zincrbyAsync('clickhous_tracking_events', 1, redisKey)
    ]);
}

function handleNoDemandsSituation(params, res){
    return returnEmptyVast(res);
    /*return ErrorHandler.handleError(redis, {
        supply: params.supplyId,
        demand: 0,
        message: `All demands blocked in Supply ${params.supplyId}`,
        err_type: 'no_show'
    }, params.referer, params.countryCode, params.partnerId)
        .catch(console.error)
        .then(() => returnEmptyVast(res));*/
}

function filterDemands(demands, params){
    return demands.filter(demand => {
        const countryCheck = CountryTargetingLib.checkDemandCountry(demand.demand, params.countryCode);
        if (!countryCheck.success){
          (extra.tagLogging || extra.logForSupplies.includes(parseInt(params.supplyId)) || extra.logForDemands.includes(parseInt(demand.demand)))
          && console.log(Object.assign(countryCheck, {supply: params.supplyId}));
            return false;
        }
        const budgetLimitCheck = BudgetLimitLib.checkBudgetLimit(demand);
        if (!budgetLimitCheck.success){
          (extra.tagLogging || extra.logForSupplies.includes(parseInt(params.supplyId)) || extra.logForDemands.includes(parseInt(demand.demand)))
          && console.log(Object.assign(budgetLimitCheck, {supply: params.supplyId}));
            return false;
        }
        const blackWhiteListCheck = BlackWhiteListsLib.checkDemandReferer(demand.demand, params.referer);
        if (!blackWhiteListCheck.success){
          (extra.tagLogging || extra.logForSupplies.includes(parseInt(params.supplyId)) || extra.logForDemands.includes(parseInt(demand.demand)))
          && console.log(Object.assign(blackWhiteListCheck, {supply: params.supplyId}));
            return false;
        }

        const deviceTargetingCheck = DeviceTargetingLib.checkDemandDevice(demand.demand, params.userAgent);
        if(!deviceTargetingCheck.success){
            (extra.tagLogging || extra.logForSupplies.includes(parseInt(params.supplyId)) || extra.logForDemands.includes(parseInt(demand.demand)))
            && console.log(Object.assign(deviceTargetingCheck, {supply: params.supplyId}));
            return false;
        }

        const playerSizeTargetingCheck = PlayerSizeTargetingLib.checkDemandPlayerSize(demand.demand, params.height);
        if(!playerSizeTargetingCheck.success){
            (extra.tagLogging || extra.logForSupplies.includes(parseInt(params.supplyId)) || extra.logForDemands.includes(parseInt(demand.demand)))
            && console.log(Object.assign(playerSizeTargetingCheck, {supply: params.supplyId}));
            return false;
        }

        return true;
    });
}

function pickDemands(demands){
    const requestsSum = demands.reduce((sum, demand) => sum + demand.requests, 0);

    let prevFinish = 0;
    const segments = demands.map((demand, index, arr) => {
        const start = prevFinish;
        const length = demand.requests / requestsSum;
        prevFinish += length;
        const finish = (index === arr.length - 1) ? 1 : (start + length);
        return {
            demand,
            start,
            finish
        }
    });
    const randomPointer = Math.random();
    const resultingDemandIndex = segments.findIndex(segment => {
        return randomPointer >= segment.start && randomPointer < segment.finish;
    });
    const matchedDemand = demands[resultingDemandIndex];
    return _.uniqBy([matchedDemand].concat(demands),'demand')
        .slice(0, DEMANDS_IN_QUEUE_PER_REQUEST);
}

function chooseOneDemandAndGetXML(demands, params){
    return new Promise(async (resolve, reject) => {
        let resultingDemandXML = null;
        let demandIndex = 0;
        while(resultingDemandXML === null && demandIndex < demands.length){
            const currentDemand = demands[demandIndex++];
            try {
                const xml = await VastParser.call({
                    link: currentDemand.vast,
                    demand: currentDemand.demand,
                    supply: params.supplyId,
                    query: params.query,
                    ip: params.ip,
                    ua: params.userAgent,
                    referer: params.referer,
                    headers: params.headers,
                    partnerId: params.partnerId,
                    device: params.device
                });
                if (xml){
                    resultingDemandXML = xml;
                    await writeEventToRedis('requests', params, currentDemand.demand);
                }
            } catch(error){
                ErrorHandler.handleError(redis, error, params.referer, params.countryCode, params.partnerId, params.device.type);
            }
        }
        resolve(resultingDemandXML);
    });

}

exports.call = async function(req, res){
    const params = {
        referer: req.query.url || req.headers.referer,
        ip: req.query.ip && isIpValid(req.query.ip)
            ? req.query.ip
            : req.headers['x-real-ip'] || req.headers['x-forwarded-for'],
        userAgent: req.query.ua || req.headers['user-agent'],
        countryCode : req.headers['country_code'] && req.headers['country_code'].toUpperCase(),
        city: req.headers['city_name'],
        partnerId: Number(req.query.trid) || 0,
        query: req.query,
        headers: req.headers,
        supplyId: Number(req.params.id),
        ip_counter: req.ip_counter,
        device: device(req.query.ua || req.headers['user-agent']),
        height: req.query.h || req.query.height || req.query.player_height
    };

    if (params.referer){
        if (typeof params.referer === 'string' && (!params.referer.startsWith('http'))){
            params.referer = 'http://' + params.referer;
        }
        try{
            params.referer = url.parse(params.referer).hostname;
        }
        catch(e){
            params.referer = 'unknown';
        }
    } else {
        params.referer = 'unknown';
    }
    if (!params.referer){
        params.referer = 'unknown';
    }

    if (!params.supplyId){
        console.error('Bad supply ID', params.supplyId, params.referer, params.ip);
        return returnEmptyVast(res);
    }


    params.referer = params.referer.replace('www.', '');

    const todaysReferersOfSupplySet = ReferersBySupplyLib.getReferersBySupplyId(params.supplyId);
    if (todaysReferersOfSupplySet
        && !todaysReferersOfSupplySet.has(params.referer)
        && todaysReferersOfSupplySet.size >= MAX_REFERERS_FOR_SUPPLY_PER_DAY){
        return returnEmptyVast(res);
    }

    const countryTargetingCheck = CountryTargetingLib.checkSupplyCountry(params.supplyId, params.countryCode);
    if (!countryTargetingCheck.success){
        return ErrorHandler.handleError(redis, countryTargetingCheck, params.referer, params.countryCode, params.partnerId, params.device.type)
            .catch(error => {
                console.error(error);
                return returnEmptyVast(res);
            })
            .then(() => returnEmptyVast(res))
    }

    const blackWhiteListCheck = BlackWhiteListsLib.checkSupplyReferer(params.supplyId, params.referer);
    if (!blackWhiteListCheck.success){
        return ErrorHandler.handleError(redis, blackWhiteListCheck, params.referer, params.countryCode, params.partnerId, params.device.type)
            .catch(error => {
                console.error(error);
                return returnEmptyVast(res);
            })
            .then(() => returnEmptyVast(res))
    }

    const playerSizesCheck = PlayerSizeTargetingLib.checkSupplyPlayerSize(params.supplyId, params.height);
    if (!playerSizesCheck.success){
        return ErrorHandler.handleError(redis, playerSizesCheck, params.referer, params.countryCode, params.partnerId, params.device.type)
            .catch(error => {
                console.error(error);
                return returnEmptyVast(res);
            })
            .then(() => returnEmptyVast(res))
    }

    const demandsCheck = DemandsLib.getTagsForSupplyAndReferer(params.supplyId, params.referer);
    if (!demandsCheck.success){
        return ErrorHandler.handleError(redis, demandsCheck, params.referer, params.countryCode, params.partnerId, params.device.type)
          .catch(error => {
            console.error(error);
            return returnEmptyVast(res);
          })
            .then(() => returnEmptyVast(res))
    }

    const filteredDemands = filterDemands(demandsCheck.demands, params);

    if (!filteredDemands.length){
        return handleNoDemandsSituation(params, res);
    }

    const pickedDemands = pickDemands(filteredDemands);
    const xml = await chooseOneDemandAndGetXML(pickedDemands, params);

    if (!xml){
        return handleNoDemandsSituation(params, res);
    }

    return returnXML(res, xml);
};
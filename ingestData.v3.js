const express = require('express');
const ingestDataRouter = express.Router();
const request = require("request-promise");
const covid19DetailDataSchema = require('./schemaCovid19Data');
const aggStateLevelSchema = require('./schemaCWSummary');
const aggHistStateLevelSchema = require('./schemaHistCWSummary');
const stateLookupSchema = require('./schemaStateLookup');

ingestDataRouter.get('', (req, res) => {

    var dataURLs = [
        "https://api.covid19india.org/raw_data1.json", //archive
        "https://api.covid19india.org/raw_data2.json", //archive
        "https://api.covid19india.org/raw_data3.json", //archive
        "https://api.covid19india.org/raw_data4.json", //archive
        "https://api.covid19india.org/raw_data5.json"
    ];

    async function asyncHandler() {
        const aggRes = await getBatchRecordCount(covid19DetailDataSchema);
        var numberOfBatches = aggRes.length;
        console.log("batch record info from raw data collection: ", aggRes);
        var startingBatchNumber = 0;
        var queryParam = {}
        
        if (numberOfBatches !== dataURLs.length) {
            console.log('Batch data out-of-sync in collection. So data need to be reloaded.') 
            
            dataURLs = dataURLs;
            startingBatchNumber = 0;
            batchToDelete = null;
        } else {
            console.log('Batch data exists in collection. So only the current batch data need to be deleted for reload.')
            queryParam.databatch = numberOfBatches-1;  //-1 because batch number starts from 0
            //console.log("match: ", match);
            startingBatchNumber = numberOfBatches-1;
            batchToDelete = numberOfBatches - 1;
            for (let i=0; i<numberOfBatches-1; i++) {dataURLs.shift()} //shift removes the elements from the begining of the array
        }
        console.log("remaining data urls: ", dataURLs);
        
        const deletedRecords = await clearRawDataCollection(queryParam);
        //const deletedRecords = await clearCollection(covid19DetailDataSchema, queryParam);
        //console.log("Deleted records from collection: ", deletedRecords);
        console.log(`Deleted records from collection - ${deletedRecords}`);

        const iterationStatus = await iteratorFunction(dataURLs, startingBatchNumber);
        console.log('Iteration status: ', iterationStatus);
        res.status(201).json({message: "Data ingestion completed.", URLs: dataURLs});
    }
    asyncHandler();
    insertStateLookupData(stateLookupSchema);
})

async function iteratorFunction(dataURLs, startingBatchNumber) {
    for (let index=0; index < dataURLs.length; index++) {
        //console.log("batch No:", index);
        console.log(`Iteration no:${index} - Batch starts`)
        const rawDataFromApi = await fetchDataFromApi(dataURLs[index]);
        //console.log(rawDataFromApi.length);
        console.log(`Iteration no:${index} - Number of records in API data - ${rawDataFromApi.length}`);

        const cleanedApiData = await cleanApiData(rawDataFromApi, startingBatchNumber);
        //onsole.log("Cleaned API data.");
        console.log(`Iteration no:${index} - Cleaned API data`);



        const insertedRecords = await insertDataInCollection(cleanedApiData);
        //console.log("Inserted records into collection: ", insertedRecords);
        console.log(`Iteration no:${index} - Inserted records into collection - ${insertedRecords}`);
        console.log(`Iteration no:${index} - Batch ends`);
        startingBatchNumber++;
    }
    var aggData;
    var cleanedAggData;
    var deletedRecs;
    //var qryParam = {};
    //state level data preparation
    var aggSpecStateStatusLevel = [
        {$group: {_id: {statecode: "$statecode", currentstatus: "$currentstatus"}, cases: {$sum: "$cases"}}}
    ];
    console.log('Creating State-Level aggregation...');
    aggData = await getAggregatedData(covid19DetailDataSchema, aggSpecStateStatusLevel);
    console.log("Aggregated the detailed data at state-level: Number of records: ", aggData.length);

    cleanedAggData = await cleanStateLevelAggData(aggData);
    console.log("Cleaned state-level agg data. Number of records: ", cleanedAggData.length);

    deletedRecs = await clearCollection(aggStateLevelSchema, {});
    console.log("Cleand up State-level Summary table. Number of records deleted: ", deletedRecs);
    
    const stateLevelSummary = await insertAggData(aggStateLevelSchema, cleanedAggData)
    console.log(`Inserted State-level summary data. Number of records: ${stateLevelSummary.length}`);

    //state level historical data preparation
    console.log('Creating Historical State-Level aggregation...');
    var aggSpecHistStateStatusLevel = [
        {$group: {_id: {date: "$date", statecode: "$statecode", currentstatus: "$currentstatus"}, cases: {$sum: "$cases"}}}
    ];
    aggData = await getAggregatedData(covid19DetailDataSchema, aggSpecHistStateStatusLevel);
    console.log("Aggregated the detailed data: Number of records: ", aggData.length);

    cleanedAggData = await cleanHistStateLevelAggData(aggData);
    console.log("Cleaned state-hist-level agg data. Number of records: ", cleanedAggData.length);

    deletedRecs = await clearCollection(aggHistStateLevelSchema, {});
    console.log("Cleand up State-hist-level Summary table. Number of records deleted: ", deletedRecs);
    
    const histStateLevelSummary = await insertAggData(aggHistStateLevelSchema, cleanedAggData)
    console.log(`Inserted State-hist-level summary data. Number of records: ${histStateLevelSummary.length}`);

    return ({stateLevelRecords: stateLevelSummary.length, histStateLevelRecords: histStateLevelSummary.length});
}

async function getBatchRecordCount(schema) {
    const aggQry = schema.aggregate(
        [
            {$group: {_id: {databatch: "$databatch"}, recordCount: {$sum: 1}}}
        ]
    )
    try {
        return aggQry.exec()
    }
    catch(err) {
        console.log("err in the aggregation...", err);
    }
}

async function fetchDataFromApi(url) {
    return new Promise((resolve, reject) => {
        var requestOptions = {uri: url, headers: {'User-Agent': 'Request-Promise'},json: true};
        request(requestOptions)
            .then(function(rawApiData) {
                resolve(rawApiData.raw_data);
            })
            .catch(function(err) {
                reject('api data fetch failed', err);
            })
    })
}


async function cleanApiData(rawApiData, i) {
    const cleanedApiDataPromises = await rawApiData.map(async rawApiDataRow => {
        var dateParts = rawApiDataRow.dateannounced.split("/");
        var myDateString = dateParts[2] + "/" + dateParts[1] + "/" + dateParts[0]
        return (
            {
                databatch: i,
                entryid: rawApiDataRow.entryid,
                date: myDateString,
                //date: rawApiDataRow.dateannounced,
                statecode: rawApiDataRow.statecode, 
                district: rawApiDataRow.detecteddistrict,
                city: rawApiDataRow.detectedcity,
                currentstatus: rawApiDataRow.currentstatus,
                cases: rawApiDataRow.numcases                
            }
        )
    })
    const cleanedApiData = await Promise.all(cleanedApiDataPromises);
    return cleanedApiData;
}

async function clearCollection(schema, matchQry) {
    return new Promise((resolve, reject) => {
        schema.deleteMany(matchQry, (err, deletedRecords) => {
            if(!err) {
                resolve(deletedRecords.n);
                console.log("match query in clear collections: ", matchQry);
            } else {
                reject(err)
            }
        })
    })
}

async function clearRawDataCollection(qryParam) {
    return new Promise((resolve, reject) => {
        covid19DetailDataSchema.deleteMany(qryParam, (err, deletedRecords) => {
            if(!err) {
                resolve(deletedRecords.n);
                //console.log("match query in clear collections: ", matchQry);
            } else {
                reject(err)
            }
        })
    })
}

async function insertDataInCollection(data) {
    return new Promise((resolve,reject) => {
        covid19DetailDataSchema.insertMany(data, (err, insertedRecords) => {
            if(!err) {
                resolve(insertedRecords.length);

            } else {
                reject (console.log('error during insertion: ', err));
            }
        })
    })
}

async function getAggregatedData(schema, aggSpec) {
    const aggQry = schema.aggregate(aggSpec)
    try {
        return aggQry.exec()
    }
    catch(err) {
        console.log("err in the aggregation...", err);
    }
}

async function cleanStateLevelAggData(detailData) {
    const cleanedDataPromises = await detailData.map(async dataRow => {
        return (
            {
            statecode: dataRow._id.statecode, 
            currentstatus: dataRow._id.currentstatus,
            cases: dataRow.cases                
            }
        )
    })
    const cleanedAggData = await Promise.all(cleanedDataPromises);
    return cleanedAggData;
}

async function cleanHistStateLevelAggData(detailData) {
    const cleanedDataPromises = await detailData.map(async dataRow => {
        return (
            {
            date: dataRow._id.date,
            statecode: dataRow._id.statecode, 
            currentstatus: dataRow._id.currentstatus,
            cases: dataRow.cases                
            }
        )
    })
    const cleanedAggData = await Promise.all(cleanedDataPromises);
    return cleanedAggData;
}

async function clearCollection(schema) {
    return new Promise((resolve, reject) => {
        schema.deleteMany({}, (err, deletedRecords) => {
            if(!err) {
                resolve(deletedRecords.n);
            } else {
                reject(err)
            }
        })
    })
}


async function insertAggData(schema, data) {
    return new Promise((resolve, reject) => {
        schema.insertMany(data, (err, insertedRecords) => {
            if(!err) {
                resolve(insertedRecords);
            } else {
                reject(console.log('State-level data insertion failed. ', err));
            }
        });
    });
}


async function insertStateLookupData(schema) {
    const stateLookupData = await stateLookupDataContainer();
    schema.find({}, (err, result) => {
        if(err) {
            console.log('error in state lookup table query', err)
        } else if(result.length === 736) {
            console.log('state table has data');
        } else {
            console.log('state table do not have right data. need to refresh. n=',result.length);
            schema.deleteMany({}, (err, deletedRecords) => {
                if(err) {console.log('error during deleting state look up table.', err)}
                else {
                    console.log('Records deleted from state table. n=', deletedRecords.n);
                    schema.insertMany(stateLookupData, (err, insertedRecords) => {
                        if(!err) {
                            console.log('records inserted into state lookup table. n=', insertedRecords.length);
                        } else {
                            console.log('error during state look up insertion', err);
                        }
                    })
                }
                
            })
            
        }
    })
}

function stateLookupDataContainer() {
    return stateLookupData = [
        {state:'Andaman and Nicobar',statecode:'AN',district:'Nicobar'},
        {state:'Andaman and Nicobar',statecode:'AN',district:'North Middle Andaman'},
        {state:'Andaman and Nicobar',statecode:'AN',district:'South Andaman'},
        {state:'Andhra Pradesh',statecode:'AP',district:'Anantapur'},
        {state:'Andhra Pradesh',statecode:'AP',district:'Chittoor'},
        {state:'Andhra Pradesh',statecode:'AP',district:'East Godavari'},
        {state:'Andhra Pradesh',statecode:'AP',district:'Guntur'},
        {state:'Andhra Pradesh',statecode:'AP',district:'Kadapa'},
        {state:'Andhra Pradesh',statecode:'AP',district:'Krishna'},
        {state:'Andhra Pradesh',statecode:'AP',district:'Kurnool'},
        {state:'Andhra Pradesh',statecode:'AP',district:'Nellore'},
        {state:'Andhra Pradesh',statecode:'AP',district:'Prakasam'},
        {state:'Andhra Pradesh',statecode:'AP',district:'Srikakulam'},
        {state:'Andhra Pradesh',statecode:'AP',district:'Visakhapatnam'},
        {state:'Andhra Pradesh',statecode:'AP',district:'Vizianagaram'},
        {state:'Andhra Pradesh',statecode:'AP',district:'West Godavari'},
        {state:'Arunachal Pradesh',statecode:'AR',district:'Anjaw'},
        {state:'Arunachal Pradesh',statecode:'AR',district:'Central Siang'},
        {state:'Arunachal Pradesh',statecode:'AR',district:'Changlang'},
        {state:'Arunachal Pradesh',statecode:'AR',district:'Dibang Valley'},
        {state:'Arunachal Pradesh',statecode:'AR',district:'East Kameng'},
        {state:'Arunachal Pradesh',statecode:'AR',district:'East Siang'},
        {state:'Arunachal Pradesh',statecode:'AR',district:'Kamle'}
    ];
}

module.exports = ingestDataRouter;
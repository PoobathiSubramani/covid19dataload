const express = require("express");
const app = express();
const mongoose = require("mongoose");
const bodyParser = require("body-parser");

const ingestDataRoute = require('./ingestData.v3');

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));

dbConnectionStatus = "not connected to DB";

dbConnectionString = "mongodb+srv://"+
process.env.MONGODB_DBUSER+":"+
process.env.MONGODB_DBPWD+"@"+
process.env.MONGODB_CLUSTER+"/"+
process.env.MONGODB_DBNAME+"?"+"retryWrites=true&w=majority";
//"@boomongocluster-rcqr2.azure.mongodb.net/node-angular?retryWrites=true&w=majority"

dbConnectionString = "mongodb+srv://dbuser:dbpwd09@boomongocluster-rcqr2.azure.mongodb.net/covid19db";//?retryWrites=true&w=majority"

mongoose
  .connect(
    dbConnectionString,
    {useNewUrlParser: true, useUnifiedTopology: true} //added as per deprication warnings
  )
  .then(() => {
    console.log('Connection to db successful')
    dbConnectionStatus = "connected to DB";
  })
  .catch((err) => {
    console.error("Connection error with DB: " ,err);
  });

  app.get("", (req, res) => {
    res.send({Message:"Server initialized and... " + dbConnectionStatus});
});

app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader(
    "Access-Control-Allow-Headers",
    "Origin, X-Requested-With, Content-Type, Accept"
  );
  res.setHeader(
    "Access-Control-Allow-Methods",
    "GET, POST, PATCH, PUT, DELETE, OPTIONS"
  );
  next();
});

app.use("/ingest", ingestDataRoute);
//app.use(ingestDataRoute);
module.exports=app;
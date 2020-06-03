const mongoose = require("mongoose");

const Covid19DataSchema = mongoose.Schema([{
    databatch: Number,
    entryid: Number,
    date: String,
    statecode: String,
    district: String,
    city: String,
	currentstatus: String,
	cases: Number
}])

module.exports = mongoose.model("covid19datacollection", Covid19DataSchema);
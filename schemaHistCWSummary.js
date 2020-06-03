const mongoose = require("mongoose");

const HistCWSummarySchema = mongoose.Schema([{
	date: String,
    statecode: String,
	currentstatus: String,
	cases: Number
}])

module.exports = mongoose.model("histcwsummarycollection", HistCWSummarySchema);
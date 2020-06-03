const mongoose = require("mongoose");

const CWSummarySchema = mongoose.Schema([{
    statecode: String,
	currentstatus: String,
	cases: Number
}])

module.exports = mongoose.model("cwsummarycollection", CWSummarySchema);
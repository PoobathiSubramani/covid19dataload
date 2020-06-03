const mongoose = require("mongoose");

const stateLookupSchema = mongoose.Schema([{
	state: String,
	statecode: String,
	district: String
}])

module.exports = mongoose.model("statedistrict", stateLookupSchema);
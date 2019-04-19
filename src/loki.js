/*
 * HEPop Loki connector
 *
 */

const log = require('./logger');
const stringify = require('safe-stable-stringify');
var config = require('./config').getConfig();
var cs;
var db;

const axios = require('axios');

if(!config.db.loki) {
    log('%stop:red Missing configuration for Loki [%s:blue]');
    process.exit();
    //return;
  }

var lokiApi = axios.create({
  baseURL: config.db.loki.url,
  timeout: 1000,
});
lokiApi.defaults.headers.post['Content-Type'] = 'application/json';

/* helpers */

const groupBy = (items, key) => items.reduce(
  (result, item) => ({
    ...result,
    [item.raw[key]]: [
      ...(result[item.raw[key]] || []),
      item,
    ],
  }), 
  {},
);

const labelFromRow = (row,type,id) => {
	label=[];
	label.push(`type="${type}"`);
	label.push(`id="${id}"`);
	for (var header in row.data_headers) {
		label.push(`${header}="${row.data_headers[header]}"`);
	}
	return '{'+label.join(', ') + '}';
};

const datasetByLabel = (dataset) => {
	var dataByLabel = {};
	for (var xid in dataset) {
		dataset[xid].forEach((row) => {
			var label = labelFromRow(row,row.raw.type,xid);
			if (!dataByLabel.hasOwnProperty(label)) {
				dataByLabel[label] = [];
			}
			dataByLabel[label].push(row);
		})
	}
	return dataByLabel;
}

var rawSize = config.db.rawSize || 8000;

// Generating a multi-row insert to /api/prom/push
exports.insert = function(bulk,id){
	if (config.debug) log('GOT LOKI BULK: %s',JSON.stringify(bulk));

	// FORM Loki API Post body
	var line = {"streams": []};
	var uniq = [];
	var count = 0;
	var groups = 0;
	var labels = "";
	var dataset = groupBy(bulk,'type');

	dataByLabel = datasetByLabel(dataset);
	if (config.debug) log('DATA BY LABEL: %s',JSON.stringify(dataByLabel));
	for (var label in dataByLabel) {
		if (config.debug) log('PROCESSING LABEL: %s',JSON.stringify(label));
		if(uniq.indexOf(label) == -1) {
			uniq[label] = count;
			line.streams.push({"labels": "", "entries": [] });
		}
		dataByLabel[label].forEach(function(row){
			line.streams[uniq[label]].entries.push({ "ts": new Date().toISOString(), "line": JSON.stringify(row.raw)  });
		});
		count++;
		line.streams[uniq[label]].labels=label;
	}
	line = JSON.stringify(line);
	// POST Bulk to Loki
	if (line){
		if (config.debug) console.log('PROCESSING LOKI BULK',line);
		lokiApi.post(config.db.loki.url, line)
		  .then(function (response) {
		    if (config.debug) console.log('LOKI RESP',response.status);
		  })
		  .catch(function (error) {
		    console.log('LOKI ERR',error);
		  });
	}

};

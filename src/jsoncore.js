const log = require('./logger');
const r_bucket = require('./bucket').bucket;
const pgp_bucket = require('./bucket').pgp_bucket;
const r = require('./bucket').r;
const stringify = require('safe-stable-stringify');
const flatten = require('flat')
const config = require('./config').getConfig();

const metrics = require('./metrics').metrics;
var mm = true;

if(!config.metrics || !config.metrics.influx){
	log('%data:red Metrics disabled');
	mm = false;
} else {
	log('%data:green Metrics enabled %s', stringify(config.metrics.influx));
}

var buckets = [];

exports.processJson = function(data,socket) {
	try {
  	  //if (config.debug) log('%data:cyan JSON Net [%s:blue][%s:green]', stringify(socket) );
	  if (!data) return;
	  data = JSON.parse(data.toString());		
  	  if (config.debug) log('%data:cyan JSON Data [%s:blue][%s:green]', stringify(data) );
	  // DB Schema
	  var insert = { "protocol_header": socket,
			 "data_header": {},
			 "raw": data || ""
		};	
	  // Create protocol bucket
	  var key = 127 + "_"+ (insert.protocol_header.type || "default");
	  if (!buckets[key]) buckets[key] = require('./bucket').pgp_bucket;
	  buckets[key].set_id("hep_proto_"+key);	
		
	  if (data.type && data.event){	
	    // Janus Media Reports
	    if (config.debug) log('%data:green JANUS REPORT [%s]',stringify(data) );
	    var tags = { session: data.session_id, handle: data.handle_id };
	    switch(data.type) {
		case 32:
		  if (data.event.media) tags.medium = data.event.media;
		  if(data.event.receiving) {
		    metrics.setGauge(metrics.gauge("janus", tags, 'Receiving') );
		  } else if(data.event.base) {
		    metrics.setGauge(metrics.gauge("janus", tags, 'LSR' ), data.event["lsr"] );
		    metrics.setGauge(metrics.gauge("janus", tags, 'lost' ), data.event["lost"] || 0 );
		    metrics.setGauge(metrics.gauge("janus", tags, 'lost-by-remote' ), data.event["lost-by-remote"] || 0 );
		    metrics.setGauge(metrics.gauge("janus", tags, 'jitter-local' ), data.event["jitter-local"] || 0 );
		    metrics.setGauge(metrics.gauge("janus", tags, 'jitter-remote' ), data.event["jitter-remote"] || 0);
	            metrics.setGauge(metrics.gauge("janus", tags, 'packets-sent' ), data.event["packets-sent"] || 0);
		    metrics.setGauge(metrics.gauge("janus", tags, 'packets-received' ), data.event["packets-sent"] || 0);
		    metrics.setGauge(metrics.gauge("janus", tags, 'bytes-sent' ), data.event["bytes-sent"] || 0);
		    metrics.setGauge(metrics.gauge("janus", tags, 'bytes-received' ), data.event["bytes-received"]|| 0 );
		    metrics.setGauge(metrics.gauge("janus", tags, 'nacks-sent' ), data.event["nacks-sent"] || 0);
		    metrics.setGauge(metrics.gauge("janus", tags, 'nacks-received' ), data.event["nacks-received"] || 0);
		  }
		break;
	    }
		  
	  } else if (data.event && data.event == 'producer.stats' &&  data.stats){	
		// MediaSoup Media Reports
		var tags = { roomId: data.roomId, peerName: data.peerName, producerId: data.producerId };
		    if (data.stats[0].mediaType) tags.media = data.stats[0].mediaType;
		    if (data.stats[0].type) tags.media = data.stats[0].type;
		    metrics.setGauge(metrics.gauge("mediasoup", tags, 'bitrate' ), data.stats[0]["bitrate"] );
		    metrics.setGauge(metrics.gauge("mediasoup", tags, 'byteCount' ), data.stats[0]["byteCount"] );
		    metrics.setGauge(metrics.gauge("mediasoup", tags, 'firCount' ), data.stats[0]["firCount"] );
		    metrics.setGauge(metrics.gauge("mediasoup", tags, 'fractionLost' ), data.stats[0]["fractionLost"] );
		    metrics.setGauge(metrics.gauge("mediasoup", tags, 'jitter' ), data.stats[0]["jitter"] );
	            metrics.setGauge(metrics.gauge("mediasoup", tags, 'nackCount' ), data.stats[0]["nackCount"] );
		    metrics.setGauge(metrics.gauge("mediasoup", tags, 'packetCount' ), data.stats[0]["packetCount"] );
		    metrics.setGauge(metrics.gauge("mediasoup", tags, 'packetsDiscarded' ), data.stats[0]["packetsDiscarded"] );
		    metrics.setGauge(metrics.gauge("mediasoup", tags, 'packetsLost' ), data.stats[0]["packetsLost"] );
		    metrics.setGauge(metrics.gauge("mediasoup", tags, 'packetsRepaired' ), data.stats[0]["packetsRepaired"] );
		    metrics.setGauge(metrics.gauge("mediasoup", tags, 'nacks-received' ), data.stats[0]["nacks-received"] );
	  }
			  
	  //if (r_bucket) r_bucket.push(JSON.parse(dec));
	  //if (pgp_bucket) pgp_bucket.push(dec);

	} catch(err) { log('%error:red %s', err.toString() ) }
};

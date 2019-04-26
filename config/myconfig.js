{
  "id" : "HEPop101",
  "socket": "http",
  "port": 8080,
  "address": "127.0.0.1",
  "tls": {
    "key":"/etc/keys/self.key",
    "cert":"/etc/keys/self.crt"
  },
  "queue": {
    "timeout": 2000,
    "maxSize": 1000,
    "useInterval": true
  },
  "dbName": "hep",
  "tableName": "hep",
  "db": {
	"rawSize": 8000,
        "loki" : {
    	  "url": "http://localhost:3100/api/prom/push"
        }
  },
  "metrics": {
	"influx":{
		"period": 30000,
		"expire": 300000,
		"dbName": "hep",
		"hostname": "localhost:8086"
	}
  },
  "db_off": {
	"rethink" : {
	  "servers":[
	    { "host": "127.0.0.1", "port":28015 }
  	  ]
	},
  	"mongodb":{
	  "url": "mongodb://localhost:27017/homer"
	},
	"elastic" : {
     	  "target": "http://localhost:9200",
     	  "max_bulk_qtty": 1000,
     	  "max_request_num": 20,
     	  "index": "hep"
        }
  },
  "debug": true
}

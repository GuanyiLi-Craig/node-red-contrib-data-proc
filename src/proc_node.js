const { off } = require('process');

module.exports = function(RED) {
    "use strict";
    const duckdb= require('duckdb');
    const childProcess = require("child_process")
    const util = require("util");
    const vm = require("vm");
    const path = require("path");
    const fs = require('fs');
    const crypto = require("crypto");

    function cleanTable(nodeId, con) {
        var deleteAll = "DELETE FROM " + getTableName(nodeId);
        getAllResult(deleteAll, con)
    }

    function createTableSql(nodeId) {
        return "CREATE TABLE IF NOT EXISTS " + getTableName(nodeId) + " (keys json PRIMARY KEY, data json);" 
    }

    function insertIntoTableSql(nodeId, keys, data) {
        return "INSERT INTO " + getTableName(nodeId) 
             + " (keys, data) VALUES('" + JSON.stringify(keys) + "', '" + JSON.stringify(data)
             + "');";
    }

    function getExecResult(query, con) {
        return new Promise(function(resolve, reject) {
            con.exec(query, function (err, rows) {
                if (err) {
                    return reject(err);
                }
                resolve(rows);
            });
        });
    }

    function getAllResult(query, con) {
        return new Promise(function(resolve, reject) {
            con.all(query, function (err, rows) {
                if (err) {
                    return reject(err);
                }
                resolve(rows);
            });
        });
    }
 
    function readCSV(filePath){
        // Read CSV
        var f = fs.readFileSync(filePath, {encoding: 'utf-8'}, 
            function(err){console.log(err);});

        // Split on row
        f = f.split("\n");

        // Get first row for column headers
        const headers = f.shift().split(",");

        var json = [];    
        f.forEach(function(d){
            // Loop through each row
            var tmp = {}
            var row = d.split(",")
            for(var i = 0; i < headers.length; i++){
                tmp[headers[i]] = row[i];
            }
            // Add object to list
            json.push(tmp);
        });
        console.log(json);
        return json;
    }

    function generateSHA512(object) {
        let sha512 = crypto.createHash('sha512').update(JSON.stringify(object)).digest('hex');
        return sha512;
    }

    function isGitDiff() {
        const proj = RED.settings.get('projects')
        const projectPath = path.join(RED.settings.userDir, 'projects', proj.activeProject)
        const command = "cd " + projectPath.toString() + " && git diff";
        const gitDiff = childProcess
            .execSync(command)
            .toString()
            .trim();

        return gitDiff !== '';
    }

    function getGitCommit() {
        const proj = RED.settings.get('projects')
        const projectPath = path.join(RED.settings.userDir, 'projects', proj.activeProject)
        const command = "cd " + projectPath.toString() + " && git rev-parse HEAD";
        const commitHash = childProcess
            .execSync(command)
            .toString()
            .trim();

        return commitHash;
    }

    function getGitBranch() {
        const proj = RED.settings.get('projects')
        const projectPath = path.join(RED.settings.userDir, 'projects', proj.activeProject)

        const command = "cd " + projectPath.toString() + " && git rev-parse --abbrev-ref HEAD";

        const branchName = childProcess
            .execSync(command)
            .toString()
            .trim();

        const branchPath = 'projects' + "/" + proj.activeProject + "/" + branchName;

        childProcess
            .execSync("mkdir -p " + branchPath);

        return branchPath;
    }

    function getTableName(nodeId) {
        var tableNameSuffix = "";
        var tableNamePrefix = "_"
        if (isGitDiff()) {
            tableNameSuffix = '_head';
        }
        return tableNamePrefix + nodeId + tableNameSuffix;
    }

    function createVMOpt(node, kind) {
        var opt = {
            filename: 'Function node'+kind+':'+node.id+(node.name?' ['+node.name+']':''),
            displayErrors: true
        };
        return opt;
    }

    function updateErrorInfo(err) {
        if (err.stack) {
            var stack = err.stack.toString();
            var m = /^([^:]+):([^:]+):(\d+).*/.exec(stack);
            if (m) {
                var line = parseInt(m[3]) -1;
                var kind = "body:";
                if (/setup/.exec(m[1])) {
                    kind = "setup:";
                }
                if (/cleanup/.exec(m[1])) {
                    kind = "cleanup:";
                }
                err.message += " ("+kind+"line "+line+")";
            }
        }
    }

    function DBSysNode(n) {
        RED.nodes.createNode(this,n);
    
        this.dbSys = n.dbSys;
        this.db = {};
        this.con = {};
        this.dbname = getGitCommit();
        this.dbpath = getGitBranch();
        var node = this;


        node.doConnect = function() {
            if (node.db[node.dbSys]) { return; }
            if (node.dbSys === "duckdb") {
                node.db[node.dbSys] = new duckdb.Database(node.dbpath + "/" + node.dbname);
            }
            if (node.con[node.dbSys]) { return; }
            if (node.dbSys === "duckdb") {
                node.con[node.dbSys] = node.db[node.dbSys].connect();
            }
        }

        node.on('close', function (done) {
            if (node.tick) { clearTimeout(node.tick); }
            if (node.con) { node.con[node.dbSys].close(done()); }
            if (node.db) { node.db[node.dbSys].close(done()); }
            else { done(); }
        });
    }
    RED.nodes.registerType("db sys",DBSysNode);

    function DataProcImport(n) {
        RED.nodes.createNode(this,n);

        this.dataimport = n.dataimport
        this.datafile = n.dataimportfile;
        this.dbsys = n.dbsys;

        var node = this;

        node.dbsys = RED.nodes.getNode(node.dbsys);
        node.dbsys.doConnect();

        node.status({fill:"green",shape:"dot"});

        var doImport = async function(msg) {
            var dbCon = node.dbsys.con[node.dbsys.dbSys];

            if (node.dataimport == "import-csv") {
                if (typeof node.datafile === 'string' && node.datafile.length > 0) {
                    var csvImportCreateTableSql = createTableSql(node.id);
                    console.log(csvImportCreateTableSql);
                    try {
                        await getAllResult(csvImportCreateTableSql, dbCon);
                        await cleanTable(node.id, dbCon);
                        const csvJson = await readCSV(node.datafile);
                        
                        var batchInsertQuery = "";

                        for(var ind = 0; ind<csvJson.length; ind++) {
                            const hash = generateSHA512(csvJson[ind]);
                            const keys = {
                                "hash": hash
                            };
                            const data = csvJson[ind];
                            var insert = insertIntoTableSql(node.id, keys, data);
                            console.log(insert);
                            batchInsertQuery = batchInsertQuery + insert + '\n';
                            if(ind % 100 == 0) {
                                await getExecResult(batchInsertQuery, dbCon);
                                batchInsertQuery = "";
                            }
                        }
                        await getExecResult(batchInsertQuery, dbCon);
                        msg.nodeId = node.id;
                        node.send(msg);
                    } catch(err) {
                        node.error(err, msg);
                    }
                }
                else {
                    node.error("SQL csv import config not set up",msg);
                    node.status({fill:"red",shape:"dot",text:"SQL import config not set up"});
                }
            }
            if (node.dataimport == "import-parquet") {
                if (typeof node.datafile === 'string' && node.datafile.length > 0) {
                    var parquetImportSql = "CREATE TABLE " + tableName + " AS SELECT * FROM read_parquet('" + node.datafile + "');";
                    try {
                        var row = await getAllResult(parquetImportSql, dbCon);
                        msg.nodeId = node.id;
                        node.send(msg);
                    } catch(err) {
                        node.error(err, msg);
                    }
                }
                else {
                    node.error("SQL parquet import config not set up",msg);
                    node.status({fill:"red",shape:"dot",text:"SQL import config not set up"});
                }
            }
        }

        node.on("input", function(msg) {
            console.log(node);
            if (msg.hasOwnProperty("extension")) {
                node.db.loadExtension(msg.extension, function(err) {
                    if (err) { node.error(err,msg); }
                    else { doImport(msg); }
                });
            }
            else { doImport(msg); }
        });
    }
    RED.nodes.registerType("proc import",DataProcImport);

    function DataProcNode(n) {
        RED.nodes.createNode(this,n);
        
        var node = this;
        node.name = n.name;
        node.dbsys = n.dbsys;
        node.dbsys = RED.nodes.getNode(node.dbsys);
        node.dataproc = n.dataproc;
        node.dataprocbatchsize = n.dataprocbatchsize;

        node.outputs = n.outputs;
        node.libs = n.libs || [];
        node.dbsys.doConnect();

        console.log(node.dbsys);

        if (RED.settings.dataProcExternalModules === false && node.libs.length > 0) {
            throw new Error(RED._("function.error.externalModuleNotAllowed"));
        }

        var functionText = "var results = null;"+
            "results = (async function(msg){ "+
            "var __msgid__ = msg._msgid;"+
            "var node = {"+
                "id:__node__.id,"+
                "name:__node__.name" +
            "};\n"+
                node.dataproc+"\n"+
            "})(msg);";

        var sandbox = {
            console:console,
            util:util,
            Buffer:Buffer,
            Date: Date,
            RED: {
                util: RED.util
            },
            __node__: {
                id: node.id,
                name: node.name
            },
            context: {
                set: function() {
                    node.context().set.apply(node,arguments);
                },
                get: function() {
                    return node.context().get.apply(node,arguments);
                },
                keys: function() {
                    return node.context().keys.apply(node,arguments);
                },
                get global() {
                    return node.context().global;
                },
                get flow() {
                    return node.context().flow;
                }
            }
        };

        const moduleLoadPromises = [];

        if (node.hasOwnProperty("libs")) {
            let moduleErrors = false;
            var modules = node.libs;
            modules.forEach(module => {
                var vname = module.hasOwnProperty("var") ? module.var : null;
                if (vname && (vname !== "")) {
                    if (sandbox.hasOwnProperty(vname) || vname === 'node') {
                        node.error(RED._("function.error.moduleNameError",{name:vname}))
                        moduleErrors = true;
                        return;
                    }
                    sandbox[vname] = null;
                    var spec = module.module;
                    if (spec && (spec !== "")) {
                        moduleLoadPromises.push(RED.import(module.module).then(lib => {
                            sandbox[vname] = lib.default;
                        }).catch(err => {
                            node.error(RED._("function.error.moduleLoadError",{module:module.spec, error:err.toString()}))
                            throw err;
                        }));
                    }
                }
            });
            if (moduleErrors) {
               throw new Error(RED._("function.error.externalModuleLoadError"));
           }
        }

        var processMessage = (() => {});

        node.on("input", function(msg) {
            processMessage(msg);
        });

        Promise.all(moduleLoadPromises).then(() => {
            var context = vm.createContext(sandbox);
            try {
                node.script = vm.createScript(functionText, createVMOpt(node, ""));
                
                processMessage = async function (msg) {
                    context.msg = msg;
                    node.script.runInContext(context);

                    var inputMsg = context.msg;
                    var batchSize = parseInt(node.dataprocbatchsize);

                    var dbCon = node.dbsys.con[node.dbsys.dbSys];
                    try {

                        // create <node.id>-head table if isGitDiff = true
                        // create <node.id> table if isGitDiff = false and not exist

                        var createTableQuery = createTableSql(node.id);

                        await getAllResult(createTableQuery, dbCon);
                        var offset = 0;
                        do {
                            var batchSQLQuery = "SELECT * FROM " + getTableName(msg.nodeId) + " LIMIT " + batchSize.toString() + " OFFSET " + offset.toString() + ";";
                            var rows = await getAllResult(batchSQLQuery, dbCon);
                            var batchResQuery = "";
                            rows.forEach(async row => {
                                var {keys, data} = inputMsg.proc(row)
                                var insert = insertIntoTableSql(node.id, keys, data);
                                batchResQuery = batchResQuery + insert + '\n';
                            });

                            await getExecResult(batchResQuery, dbCon);
                            offset = offset + batchSize;
                        } while (rows.length == batchSize)
                        msg.nodeId = node.id;
                        node.send(msg);
                    } catch(err) {
                        node.error(err, msg);
                        return;
                    }
                }
            }
            catch(err) {
                updateErrorInfo(err);
                node.error(err);
            }
        }).catch(err => {
            node.error(RED._("function.error.externalModuleLoadError"));
        });
        
    }
    RED.nodes.registerType("data proc", DataProcNode, {
        dynamicModuleList: "libs",
        settings: {
            dataProcExternalModules: { value: true, exportable: true }
        }
    });
}
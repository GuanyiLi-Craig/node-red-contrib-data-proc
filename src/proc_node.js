const { off } = require('process');

module.exports = function(RED) {
    "use strict";
    var duckdb= require('duckdb');
    var childProcess = require("child_process")
    var util = require("util");
    var vm = require("vm");
    var path = require("path")

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

    function doConnect(node) {
        if (node.db) { return; }
        node.db = new duckdb.Database(node.dbpath + "/" + node.dbname);
        if (node.con) { return; }
        node.con = node.db.connect();
    }

    function doClose(node) {
        if (node.tick) { clearTimeout(node.tick); }
        if (node.db) { node.db.close(); }
    };

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

    function DuckdbProcNode(n) {
        RED.nodes.createNode(this,n);
        
        var node = this;
        node.name = n.name;
        node.dbname = getGitCommit();
        node.dbpath = getGitBranch();

        node.duckdbproc = n.duckdbproc;
        node.duckdbprocbatchsize = n.duckdbprocbatchsize;

        node.outputs = n.outputs;
        node.libs = n.libs || [];

        doConnect(node);

        if (RED.settings.duckdbProcExternalModules === false && node.libs.length > 0) {
            throw new Error(RED._("function.error.externalModuleNotAllowed"));
        }

        var functionText = "var results = null;"+
            "results = (async function(msg){ "+
            "var __msgid__ = msg._msgid;"+
            "var node = {"+
                "id:__node__.id,"+
                "name:__node__.name" +
            "};\n"+
                node.duckdbproc+"\n"+
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

        node.on("close", function() {
            doClose(node);
        });

        Promise.all(moduleLoadPromises).then(() => {
            var context = vm.createContext(sandbox);
            try {
                node.script = vm.createScript(functionText, createVMOpt(node, ""));
                doConnect(node);
                processMessage = async function (msg) {
                    context.msg = msg;
                    node.script.runInContext(context);

                    var inputMsg = context.msg;
                    var batchSize = parseInt(node.duckdbprocbatchsize);

                    try {

                        // create <node.id>-head table if isGitDiff = true
                        // create <node.id> table if isGitDiff = false and not exist
                        var tableNameSuffix = "";
                        if (isGitDiff()) {
                            tableNameSuffix = '_head';
                        }
                        var createTableQuery = "CREATE TABLE IF NOT EXISTS " + node.id + tableNameSuffix + " (keys json PRIMARY KEY, data json);" 
                        console.log(createTableQuery);
                        await getAllResult(createTableQuery, node.con);
                        var offset = 0;
                        do {
                            var batchSQLQuery = "SELECT * FROM " + msg.nodeId + tableNameSuffix + " LIMIT " + batchSize.toString() + " OFFSET " + offset.toString() + ";";
                            var rows = await getAllResult(batchSQLQuery, node.con);
                            var batchResQuery = "";
                            rows.forEach(async row => {
                                var {keys, data} = inputMsg.proc(row)
                                var insert = "INSERT INTO " + node.id + tableNameSuffix + " (keys, data) VALUES(" + keys + ", " + data + ") ON DUPLICATE KEY UPDATE keys = " + keys + ";";
                                batchResQuery = batchResQuery + insert + '\n';
                            });

                            await getExecResult(batchResQuery, node.con);
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
        }).finally(() => {
            doClose(node);
        });
        
    }
    RED.nodes.registerType("duckdb proc", DuckdbProcNode, {
        dynamicModuleList: "libs",
        settings: {
            duckdbProcExternalModules: { value: true, exportable: true }
        }
    });
}
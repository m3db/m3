// we can set cluster as a top level argument when running jsonnet
local cluster = std.extVar("cluster");

// we need to import all valid envs bc import paths cannot be generated on the fly
local environments = {
    "gcp": (import "../environments/gcp.libsonnet"),
};

local env_vars = environments[cluster];
local globals = env_vars.globals + env_vars.m3em_agent.globals;

std.prune({
    server: {
        listenAddress: "0.0.0.0:" + globals.port,
        debugAddress: "0.0.0.0:" + globals.debug_port,
        // skip TLS
    },
    metrics: {
        sampleRate: 0.02,
        m3: {  
            hostPort: globals.m3_address,
            service: "m3em_agent",
            includeHost: true,
            env: "cloud",
        },
    },
    agent: {
        workingDir: globals.working_dir,
        startupCmds: if "startup_cmds" in globals then [
            {
                path: cmd.path,
                args: if "args" in cmd then cmd.args else []
            },
            for cmd in globals.startup_cmds 
        ] else [],
        releaseCmds: if "release_cmds" in globals then [
            {
                path: cmd.path,
                args: if "args" in cmd then cmd.args else []
            }, 
            for cmd in globals.release_cmds 
        ] else [],
        testEnvVars: if "env_vars" in globals then [
            {
                [var.key]: var.value,
            },  
            for var in globals.env_vars
        ] else [], 
    },
})

local cluster = "gcp";

// we need to import all valid envs bc import paths cannot be generated on the fly
local environments = {
    "gcp": (import "../environments/gcp.libsonnet"),
};

local env_vars = environments[cluster];
local globals = env_vars.globals;

// TODO: overwrite vars in globals with service layer globals instead of having
// two different sets of globals
local m3em_agent_globals = env_vars.m3em_agent.globals;
std.prune({
    server: {
        listenAddress: "0.0.0.0:" + m3em_agent_globals.port,
        debugAddress: "0.0.0.0:" + m3em_agent_globals.debug_port,
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
        workingDir: m3em_agent_globals.working_dir,
        startupCmds: if "startup_cmds" in m3em_agent_globals then [
            {
                path: cmd.path,
                args: if "args" in cmd then cmd.args else []
            },
            for cmd in m3em_agent_globals.startup_cmds 
        ] else [],
        releaseCmds: if "release_cmds" in m3em_agent_globals then [
            {
                path: cmd.path,
                args: if "args" in cmd then cmd.args else []
            }, 
            for cmd in m3em_agent_globals.release_cmds 
        ] else [],
        testEnvVars: if "env_vars" in m3em_agent_globals then [
            {
                [var.key]: var.value,
            },  
            for var in m3em_agent_globals.env_vars
        ] else [], 
    },
})

-module(mzb_api_provision).

-export([
    provision_nodes/2,
    clean_nodes/2,
    ensure_file_content/5,
    ensure_dir/4
]).

-include_lib("mzbench_language/include/mzbl_types.hrl").

-define(MICROSEC_IN_SEC, 1000000).

provision_nodes(Config, Logger) ->
    #{
        provision_nodes := ProvisionNodes,
        director_host := DirectorHost,
        worker_hosts := WorkerHosts,
        user_name := UserName,
        env := Env
    } = Config,

    UniqHosts = lists:usort([DirectorHost|WorkerHosts]),
    Logger(info, "Provisioning nodes: ~p~nWith config: ~p", [UniqHosts, Config]),
    RootDir = mzb_api_bench:remote_path("", Config),
    ok = ensure_dir(UserName, UniqHosts, RootDir, Logger),

    TimeDifferences = ntp_check(UserName, UniqHosts, Logger),

    NodeDeployPath = mzb_api_paths:node_deployment_path(),

    catch mzb_subprocess:remote_cmd(
        UserName, UniqHosts,
        io_lib:format(
            "ps -ef | grep beam | grep -v grep | grep -v mzbench_api && ~s/mzbench/bin/mzbench stop; true",
            [NodeDeployPath]),
        [], Logger),

    case ProvisionNodes of
        true ->
            ok = install_node(UniqHosts, Config, Logger),
            install_workers(UniqHosts, Config, Logger, Env);
        _ -> ok
    end,
    DirectorNode = nodename(director_sname(Config), 0),
    WorkerNodes = [nodename(worker_sname(Config), N) || N <- lists:seq(1, length(WorkerHosts))],
    Nodes = [DirectorNode | WorkerNodes],
    ensure_vm_args([DirectorHost|WorkerHosts], Nodes, Config, TimeDifferences, Logger),
    _ = mzb_subprocess:remote_cmd(
        UserName,
        [DirectorHost|WorkerHosts],
        io_lib:format("cd ~s && ~s/mzbench/bin/mzbench start", [RootDir, NodeDeployPath]),
        [],
        Logger),

    {lists:zip(Nodes, [DirectorHost|WorkerHosts]), get_management_port(Config, Logger)}.

get_management_port(Config = #{director_host:= DirectorHost, user_name:= UserName}, Logger) ->
    [Res] = mzb_subprocess:remote_cmd(
                UserName,
                [DirectorHost],
                io_lib:format("~s/mzbench/bin/nodetool", [mzb_api_paths:node_deployment_path()]),
                ["-name", nodename(director_sname(Config), 0), "rpcterms", "mzb_management_tcp_protocol", "get_port", "\\\"\\\""],
                Logger, []),
    Logger(info, "Management port: ~s", [Res]),
    erlang:list_to_integer(Res).

-spec clean_nodes(term(), fun()) -> ok.
clean_nodes(Config, Logger) ->
    #{
        user_name:= UserName,
        director_host:= DirectorHost,
        worker_hosts:= WorkerHosts} = Config,
    RootDir = mzb_api_bench:remote_path("", Config),
    _ = mzb_subprocess:remote_cmd(
        UserName,
        [DirectorHost|WorkerHosts],
        io_lib:format("cd ~s; ~s/mzbench/bin/mzbench stop; true",
            [RootDir, mzb_api_paths:node_deployment_path()]),
        [],
        Logger),
    length(RootDir) > 1 andalso mzb_subprocess:remote_cmd(UserName, [DirectorHost|WorkerHosts], io_lib:format("rm -rf ~s", [RootDir]), [], Logger),
    ok.

ntp_check(_, [H], Logger) ->
    Logger(info, "There's only one host, no need to make ntp check", []),
    [{H, 0}];
ntp_check(UserName, Hosts, Logger) ->
    MaxTimeDiff = application:get_env(mzbench_api, ntp_max_timediff_s, undefined),
    try lists:map(fun(X) ->
        Logger(info, "ntpdate response: ~p", [X]),
        [_, T | _] = lists:reverse(string:tokens(X, " \n")),
        {F, []} = string:to_float(T), erlang:round(?MICROSEC_IN_SEC*F) end,
        mzb_subprocess:remote_cmd(UserName, Hosts, "ntpdate -q pool.ntp.org", [], Logger)) of
        Offsets ->
            TimeDiff = lists:max(Offsets) - lists:min(Offsets),
            Logger(info, "NTP time diffs are: ~p, max distance is ~p microsecond", [Offsets, TimeDiff]),
            case MaxTimeDiff of
                undefined -> ok;
                _  when ?MICROSEC_IN_SEC * MaxTimeDiff >= TimeDiff -> ok;
                _ ->
                    Logger(error, "NTP CHECK FAILED, max time different is ~p microseconds", [TimeDiff]),
                    erlang:error({ntp_check_failed, TimeDiff})
            end,
            lists:zip(Hosts, Offsets)
    catch
        _:Error when MaxTimeDiff == undefined ->
            Logger(error, "NTP check crashed: ~p~n~p", [Error, erlang:get_stacktrace()]),
            [{H, undefined} || H <- Hosts];
        _:Error ->
            Logger(error, "NTP check crashed: ~p~n~p", [Error, erlang:get_stacktrace()]),
            erlang:error({ntp_call_crashed, Error})
    end.

nodename(Name, N) ->
    erlang:list_to_atom(mzb_string:format("~s_~b@127.0.0.1", [Name, N])).

ensure_vm_args(Hosts, Nodenames, Config, TimeDifferences, Logger) ->
    _ = mzb_lists:pmap(
        fun ({H, N}) ->
            TimeOffset = proplists:get_value(H, TimeDifferences),
            ensure_file_content([H], vm_args_content(N, TimeOffset, Config), "vm.args", Config, Logger)
        end, lists:zip(Hosts, Nodenames)),
    ok.

ensure_file_content(Hosts, Content, Filepath,
                    #{user_name:= UserName} = Config, Logger) ->
    Localfile = mzb_file:tmp_filename(),
    Remotefile =
        case Filepath of
            "~/" ++ _ -> Filepath;
            _ -> mzb_api_bench:remote_path(Filepath, Config)
        end,
    Logger(debug, "Ensure file content on hosts: ~p~nLocal filename: ~p~nContent: ~s~nRemote path: ~p", [Hosts, Localfile, Content, Remotefile]),
    ok = file:write_file(Localfile, Content),
    ok = ensure_file(UserName, Hosts, Localfile, Remotefile, Logger),
    ok = file:delete(Localfile).

ensure_file(UserName, Hosts, LocalPath, RemotePath, Logger) ->
    _ = mzb_lists:pmap(
        fun (Local) when Local == "localhost"; Local == "127.0.0.1" ->
                Logger(info, "[ COPY ] ~s -> ~s", [LocalPath, RemotePath]),
                {ok, _} = file:copy(LocalPath, RemotePath);
            (Host) ->
                UserNameParam =
                    case UserName of
                        undefined -> "";
                        _ -> io_lib:format("~s@", [UserName])
                    end,
                mzb_subprocess:exec_format("scp -o StrictHostKeyChecking=no ~s ~s~s:~s", [LocalPath, UserNameParam, Host, RemotePath], [stderr_to_stdout], Logger)
        end, Hosts),
    ok.

-spec ensure_dir(undefined | string(), [string()], string(), fun((atom(), string(), [term()]) -> ok)) -> ok.
ensure_dir(_User, [Local], Dir, Logger) when Local == "localhost"; Local == "127.0.0.1" ->
    Logger(info, "[ MKDIR ] ~s", [Dir]),
    % The trailing slash is needed, otherwise it will only
    % create Dir's parent, but not Dir itself
    ok = filelib:ensure_dir(Dir ++ "/");
ensure_dir(User, Hosts, Dir, Logger) ->
    _ = mzb_subprocess:remote_cmd(User, Hosts, "mkdir", ["-p", Dir], Logger, [stderr_to_stdout]),
    ok.

director_sname(#{id:= Id}) -> "mzb_director" ++ integer_to_list(Id).
worker_sname(#{id:= Id})   -> "mzb_worker" ++ integer_to_list(Id).

vm_args_content(NodeName, TimeOffset, #{node_log_port:= LogPort, node_management_port:= Port,
    vm_args:= ConfigArgs, node_log_user_port:= LogUserPort} = Config) ->
    UpdateIntervalMs = mzb_bc:maps_get(metric_update_interval_ms, Config, undefined),
    NewArgs =
        [mzb_string:format("-name ~s", [NodeName]),
         mzb_string:format("-mzbench node_management_port ~b", [Port]),
         mzb_string:format("-mzbench node_log_port ~b", [LogPort]),
         mzb_string:format("-mzbench node_log_user_port ~b", [LogUserPort])] ++
        [mzb_string:format("-mzbench node_time_offset ~b", [TimeOffset]) || TimeOffset /= undefined] ++
        [mzb_string:format("-mzbench metric_update_interval_ms ~b", [UpdateIntervalMs]) || UpdateIntervalMs /= undefined],

    io_lib:format(string:join([A ++ "~n" || A <- NewArgs ++ ConfigArgs], ""), []).

get_host_os_id(UserName, Host, Logger) ->
    string:to_lower(mzb_string:char_substitute(lists:flatten(mzb_subprocess:remote_cmd(UserName, [Host], "uname -sr", [], Logger, [])), $ , $-)).

get_host_erts_version(UserName, Host, Logger) ->
    lists:flatten(mzb_subprocess:remote_cmd(UserName, [Host], 
        "erl -noshell -eval 'io:fwrite(\\\"~s\\\", [erlang:system_info(version)]).' -s erlang halt", 
        [], Logger, [])).

get_host_system_id(UserName, Host, Logger) ->
    OSId = get_host_os_id(UserName, Host, Logger),
    ERTSVersion = get_host_erts_version(UserName, Host, Logger),
    mzb_string:format("~s_erts-~s", [OSId, ERTSVersion]).

download_file(User, Host, FromFile, ToFile, Logger) ->
    _ = case Host of
        Local when Local == "localhost"; Local == "127.0.0.1" ->
            Logger(info, "[ COPY ] ~s <- ~s", [ToFile, FromFile]),
            {ok, _} = file:copy(FromFile, ToFile);
        _ ->
            UserNameParam =
                case User of
                    undefined -> "";
                    _ -> io_lib:format("~s@", [User])
                end,
            TmpFile = mzb_file:tmp_filename(),
            _ = mzb_subprocess:exec_format("scp -o StrictHostKeyChecking=no ~s~s:~s ~s",
                [UserNameParam, [Host], FromFile, TmpFile], [stderr_to_stdout], Logger),
            Logger(info, "[ MV ] ~s -> ~s", [TmpFile, ToFile]),
            ok = file:rename(TmpFile, ToFile)
    end,
    ok.

-spec install_package([string()], string(), install_spec(), string(), term(), fun()) -> ok.
install_package(Hosts, PackageName, InstallSpec, InstallationDir, Config, Logger) ->
    Version = case InstallSpec of
        #git_install_spec{repo = Repo, branch = Branch} ->
            mzb_git:get_git_short_sha1(Repo, Branch, Logger);
        #rsync_install_spec{} ->
            {A, B, C} = os:timestamp(),
            mzb_string:format("~p.~p.~p", [A, B, C])
    end,
    #{user_name:= User} = Config,
    PackagesDir = mzb_api_paths:tgz_packages_dir(),
    HostsAndOSs = case InstallSpec of
        #git_install_spec{build = "local"} -> [{H, "noarch"} || H <- Hosts];
        _ -> mzb_lists:pmap(fun (Host) -> {Host, get_host_system_id(User, Host, Logger)} end, Hosts)
    end,
    ok = filelib:ensure_dir(PackagesDir ++ "/"),
    UniqueOSs = lists:usort([OS || {_Host, OS} <- HostsAndOSs]),
    NeededTarballs =
        [{OS, filename:join(PackagesDir, mzb_string:format("~s-~s-~s.tgz", [PackageName, Version, OS]))}
        || OS <- UniqueOSs],
    MissingTarballs = [{OS, T} || {OS, T} <- NeededTarballs, not filelib:is_file(T)],
    Logger(info, "Missing tarballs: ~p", [MissingTarballs]),
    OSsWithMissingTarballs = case [OS || {OS, _} <- MissingTarballs] of
        ["noarch"] -> Logger(info, "Building package ~s on api server", [PackageName]),
                      [TarballPath] = [T || {_, T} <- MissingTarballs],
                      build_package_on_host("127.0.0.1", User, TarballPath, InstallSpec, Logger),
                      [];
                 L -> L
        end,
    _ = mzb_lists:pmap(fun({Host, OS}) ->
            {OS, LocalTarballPath} = lists:keyfind(OS, 1, NeededTarballs),
            RemoteTarballPath = mzb_file:tmp_filename() ++ ".tgz",
            ExtractDir = mzb_file:tmp_filename(),
            try
                case lists:member(OS, OSsWithMissingTarballs) of
                    true ->
                        Logger(info, "Building package ~s on ~s", [PackageName, Host]),
                        build_package_on_host(Host, User, RemoteTarballPath, InstallSpec, Logger),
                        case lists:keyfind(OS, 2, HostsAndOSs) of
                            {Host, OS} ->
                                Logger(info, "Downloading package ~s from ~s", [PackageName, Host]),
                                download_file(User, Host, RemoteTarballPath, LocalTarballPath, Logger);
                            _ ->
                                Logger(info, "Not downloading package ~s from ~s", [PackageName, Host]),
                                ok
                        end;
                    false ->
                        Logger(info, "Uploading package ~s to ~s", [PackageName, Host]),
                        ensure_file(User, [Host], LocalTarballPath, RemoteTarballPath, Logger)
                end,
                % Extract tgz to tmp directory and then rsync it to the installation directory in order to prevent
                % different nodes provisioning to affect each other (if we ran several nodes on one host)
                ExtractCmd = mzb_string:format("mkdir -p ~s && cd ~s && tar xzf ~s", [ExtractDir, ExtractDir, RemoteTarballPath]),
                _ = mzb_subprocess:remote_cmd(User, [Host], ExtractCmd, [], Logger),
                InstallationCmd = mzb_string:format("mkdir -p ~s && rsync -aW ~s/ ~s", [InstallationDir, ExtractDir, InstallationDir]),
                _ = mzb_subprocess:remote_cmd(User, [Host], InstallationCmd, [], Logger)
            after
                RemoveCmd = mzb_string:format("rm -rf ~s; rm -rf ~s; true", [RemoteTarballPath, ExtractDir]),
                _ = mzb_subprocess:remote_cmd(User, [Host], RemoveCmd, [], Logger)
            end
        end,
        HostsAndOSs),
    ok.

build_package_on_host(Host, User, RemoteTarballPath, InstallSpec, Logger) ->
    DeploymentDirectory = mzb_file:tmp_filename(),
    CloneAndCDCommand = case InstallSpec of
        #git_install_spec{repo = GitRepo, branch = GitBranch, dir = GitSubDir} ->
            mzb_string:format("git clone ~s deployment_code && cd deployment_code && git checkout ~s && cd ./~s", [GitRepo, GitBranch, GitSubDir]);
        #rsync_install_spec{remote = Remote, excludes = Excludes, dir = SubDir} ->
            mzb_string:format("rsync -aW ~s ~s deployment_code && cd deployment_code/~s",
                [string:join(["--exclude=" ++ E || E <- Excludes], " "), Remote, SubDir])
    end,
    GenerationCmd = io_lib:format("mkdir -p ~s && cd ~s && ~s "
                                  "&& make generate_tgz && mv *.tgz ~s",
                                  [DeploymentDirectory, DeploymentDirectory,
                                   CloneAndCDCommand, RemoteTarballPath]),
    _ = mzb_subprocess:remote_cmd(User, [Host], GenerationCmd, [], Logger),
    ok.

install_node(Hosts, #{node_install_spec:= InstallSpec} = Config, Logger) ->
    install_package(
        Hosts,
        "node",
        InstallSpec,
        application:get_env(mzbench_api, node_deployment_path, ""),
        Config,
        Logger).

-spec get_worker_name(install_spec()) -> string().
get_worker_name(#git_install_spec{repo = GitRepo, dir = GitSubDir}) ->
    Base = filename:basename(GitSubDir),
    case re:replace(Base, "\\W", "", [global, {return, list}]) of
        [] -> filename:basename(GitRepo, ".git");
        BaseSanitized -> BaseSanitized
    end;
get_worker_name(#rsync_install_spec{remote = Remote, dir = ""}) -> filename:basename(Remote);
get_worker_name(#rsync_install_spec{dir = SubDir}) -> filename:basename(SubDir).

install_worker(Hosts, InstallSpec, Config, Logger) ->
    WorkerName = get_worker_name(InstallSpec),
    install_package(Hosts, WorkerName, InstallSpec, application:get_env(mzbench_api, worker_deployment_path, ""), Config, Logger).

install_workers(Hosts, #{script:= Script} = Config, Logger, Env) ->
    #{ body := Body } = Script,
    AST = mzbl_script:read_from_string(binary_to_list(Body)),
    NormEnv = mzbl_script:normalize_env(Env),
    _ = [install_worker(Hosts, IS, Config, Logger) || IS <- mzbl_script:extract_install_specs(AST, NormEnv)],
    ok.


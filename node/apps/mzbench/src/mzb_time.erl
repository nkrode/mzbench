-module(mzb_time).

-export([start_link/0,
         timestamp/0,
         get_offset/0,
         update_time_offset/0]).

-behaviour(gen_server).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
}).

%%%===================================================================
%%% API
%%%===================================================================
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec timestamp() -> {non_neg_integer(), non_neg_integer(), non_neg_integer()}.
timestamp() ->
    {MegaSecs, Secs, MicroSecs} = os:timestamp(),
    {MegaSecs, Secs, erlang:trunc(MicroSecs + get_offset())}.

-spec get_offset() -> integer().
get_offset() ->
    case ets:lookup(?MODULE, offset) of
        [{offset, Offset}] -> Offset;
        _ -> 0
    end.

-spec update_time_offset() -> ok.
update_time_offset() ->
    gen_server:call(?MODULE, update_time_offset).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
-spec init([]) -> {ok, #state{}}.
init([]) ->
    _ = ets:new(?MODULE, [set, named_table, {read_concurrency, true}]),
    ets:insert(?MODULE, {offset, 0}),
    {ok, #state{}}.

-spec handle_call(term(), {pid(), term()}, #state{}) -> term().
handle_call(update_time_offset, _From, State) ->
    {Offset, RoundTripTime} = lists:foldl(
        fun (_Attempt, {CurOffset, MinRTT}) ->
            LocalTimestamp1 = os:timestamp(),
            DirectorTimestamp = mzb_interconnect:call_director(get_local_timestamp),
            LocalTimestamp2 = os:timestamp(),

            RTT = timer:now_diff(LocalTimestamp2, LocalTimestamp1),
            Offset = timer:now_diff(DirectorTimestamp, LocalTimestamp1) - RTT div 2,
            system_log:info("Time reconciliation at ~p, round: ~p, result: ~b(~b)", [node(), _Attempt, Offset, RTT]),
            timer:sleep(200),
            case RTT < MinRTT of
                true -> {Offset, RTT};
                false -> {CurOffset, MinRTT}
            end
        end, {undefined, undefined}, lists:seq(1, 10)),

    system_log:info("[ mzb_time ] Timestamp offset between the node ~p and the director is ~p microseconds / error: ~p", [erlang:node(), Offset, RoundTripTime div 2]),
    _ = ets:update_element(?MODULE, offset, {2, Offset}),
    {reply, ok, State};
handle_call(Req, _From, State) ->
    system_log:error("Unhandled call: ~p", [Req]),
    {stop, {unhandled_call, Req}, State}.

-spec handle_cast(term(), #state{}) -> term().
handle_cast(Msg, State) ->
    system_log:error("Unhandled cast: ~p", [Msg]),
    {stop, {unhandled_cast, Msg}, State}.

-spec handle_info(timeout | term(), #state{}) -> term().
handle_info(Info, State) ->
    system_log:error("Unhandled info: ~p", [Info]),
    {noreply, State}.

-spec terminate(term(), #state{}) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(term(), #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

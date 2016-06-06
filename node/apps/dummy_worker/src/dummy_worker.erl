-module(dummy_worker).

-export([initial_state/0, metrics/0,
         print/3, test_method/3, test_pre_hook/1, doubled_print_counter/0,
         send_token/2, send_normalized_token/2]).

-type state() :: string().

-spec initial_state() -> state().
initial_state() -> "".

metrics() ->
    [{"print", counter, #{visibility => false, rps_visibility => true}},
     {"dummy", histogram, #{visibility => true}},
     {"derived", derived, #{resolver => doubled_print_counter, visibility => false}}].

doubled_print_counter() ->
    2 * mzb_metrics:get_value("print").

print(State, _Meta, Text) ->
    Start = os:timestamp(),
    _ = mzb_metrics:notify("print", 1),
    lager:info("Dummy print: ~p", [Text]),
    Finish = os:timestamp(),
    _ = mzb_metrics:notify({"dummy", histogram}, timer:now_diff(Finish, Start)),
    {nil, State}.

send_token(State, _Meta) ->
    Nodes = lists:usort([node()|mzb_interconnect:nodes()]),
    Target = lists:nth(random:uniform(length(Nodes)), Nodes),
    mzb_interconnect:cast(Target, {token, node(), os:timestamp()}),
    {nil, State}.

send_normalized_token(State, _Meta) ->
    Nodes = lists:usort([node()|mzb_interconnect:nodes()]),
    Target = lists:nth(random:uniform(length(Nodes)), Nodes),
    mzb_interconnect:cast(Target, {normalized_token, node(), os:timestamp()}),
    {nil, State}.

test_method(State, _Meta, Text) ->
    {nil, Text ++ State}.

test_pre_hook(Env) ->
    {ok, [{"foo", "bar"} | Env]}.

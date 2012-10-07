%% Copyright 2012 Opscode, Inc. All Rights Reserved.
%% Copyright 2012 Omar Yasin All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%

-module(itest).

-exports([setup_env/0, basic_test_/0,
          statements/1]).

-include_lib("eunit/include/eunit.hrl").
-include("sqerl.hrl").

-record(user, {id, first_name, last_name, high_score, active}).

-define(GET_ARG(Name, Args), proplists:get_value(Name, Args)).
-define(NAMES, [["Kevin", "Smith", 666, <<"2011-10-01 16:47:46">>, true],
                ["Mark", "Anderson", 42, <<"2011-10-02 16:47:46">>, true],
                ["Chris", "Maier", 0, <<"2011-10-03 16:47:46">>, true],
                ["Elvis", "Presley", 16, <<"2011-10-04 16:47:46">>, false]]).

-compile([export_all]).

get_db_type() ->
    {ok, [[Type]]} = init:get_argument(db_type),
    list_to_atom(Type).

read_db_config() ->
    Type = get_db_type(),
    Path = filename:join([filename:dirname(code:which(?MODULE)), atom_to_list(Type) ++ ".config"]),
    {ok, Config} = file:consult(Path),
    Config.

setup_env() ->
    Info = read_db_config(),
    Type = get_db_type(),
    ok = application:set_env(sqerl, db_type, Type),
    ok = application:set_env(sqerl, db_host, ?GET_ARG(host, Info)),
    ok = application:set_env(sqerl, db_port, ?GET_ARG(port, Info)),
    ok = application:set_env(sqerl, db_user, "itest"),
    ok = application:set_env(sqerl, db_pass, "itest"),
    ok = application:set_env(sqerl, db_name, ?GET_ARG(db, Info)),
    %% we could also call it like this:
    %% {prepared_statements, statements(Type)},
    %% {prepared_statements, "itest/statements_pgsql.conf"},
    ok = application:set_env(sqerl, prepared_statements, {?MODULE, statements, [Type]}),
    ColumnTransforms = case Type of
                           pgsql ->
                               [{<<"created">>,
                                 fun sqerl_transformers:convert_YMDHMS_tuple_to_datetime/1}]
                       end,
    ok = application:set_env(sqerl, column_transforms, ColumnTransforms),
    application:start(crypto),
    application:start(public_key),
    application:start(ssl),
    application:start(epgsql).

statements(pgsql) ->
    {ok, Statements} = file:consult("itest/statements_pgsql.conf"),
    Statements.

basic_test_() ->
    setup_env(),
    Status = application:start(sqerl),
    %% sqerl should start or already be running for each test
    ?assert(lists:member(Status, [ok, {error, {already_started, sqerl}}])),
    {foreach,
     fun() -> error_logger:tty(true) end,
     fun(_) -> error_logger:tty(true) end,
     [
      {<<"Insert operations">>,
       fun insert_data/0},
      {<<"Select operations">>,
       fun select_data/0},
      {<<"Select w/record xform operations">>,
       fun select_data_as_record/0},
      {<<"Ensure a select that returns the number zero doesn't come back as 'none'">>,
       fun select_first_number_zero/0},
      {<<"Update blob type">>,
       fun update_datablob/0},
      {<<"Select blob type">>,
       fun select_datablob/0},
      {<<"Select boolean">>,
       fun select_boolean/0},

      {<<"Update timestamp type">>,
       fun update_created/0},
      {<<"Select timestamp type">>,
       fun select_created_by_lname/0},
      {<<"Select timestamp type">>,
       fun select_lname_by_created/0},

      {<<"Delete operation">>,
       fun delete_data/0}
     ]}.

get_db() ->
    {ok, Host} = application:get_env(sqerl, db_host),
    {ok, Port} = application:get_env(sqerl, db_port),
    {ok, Database} = application:get_env(sqerl, db_name),
    {ok, P} = sqerl_client:start_link(pgsql, Database, Host, "itest", "itest", [{port,Port}]),
    P.

execute(Statement, Connection) ->
    Statement(Connection).

insert_data() ->
    Connection = get_db(),
    Expected = lists:duplicate(4, {ok, 1}),
    ?assertMatch(Expected, [execute(sqerl:statement(new_user, Name), Connection) || Name <- ?NAMES]),
    sqerl_client:close(Connection).

select_data() ->
    Connection = get_db(),
    {ok, User} = execute(sqerl:select(find_user_by_lname, ["Smith"], first), Connection),
    sqerl_client:close(Connection),
    ?assertMatch(<<"Kevin">>, proplists:get_value(<<"first_name">>, User)),
    ?assertMatch(<<"Smith">>, proplists:get_value(<<"last_name">>, User)),
    ?assertEqual(666, proplists:get_value(<<"high_score">>, User)),
    ?assertEqual(true, proplists:get_value(<<"active">>, User)),
    ?assert(is_integer(proplists:get_value(<<"id">>, User))).

select_data_as_record() ->
    Connection = get_db(),
    {ok, User} = execute(sqerl:select(find_user_by_lname, ["Smith"], ?FIRST(user)), Connection),
    sqerl_client:close(Connection),
    ?assertMatch(<<"Kevin">>, User#user.first_name),
    ?assertMatch(<<"Smith">>, User#user.last_name),
    ?assertEqual(666, User#user.high_score),
    ?assertEqual(true, User#user.active),
    ?assert(is_integer(User#user.id)).

select_first_number_zero() ->
    Connection = get_db(),
    Expected = [{ok, 666}, {ok, 42}, {ok, 0}, {ok, 16} ],
    Returned =  [execute(sqerl:select(find_score_by_lname, [LName], first_as_scalar, [high_score]), Connection) ||
                    [_, LName, _, _, _] <- ?NAMES],
    sqerl_client:close(Connection),
    ?assertMatch(Expected, Returned).

delete_data() ->
    Connection = get_db(),
    Expected = lists:duplicate(4, {ok, 1}),
    ?assertMatch(Expected, [execute(sqerl:statement(delete_user_by_lname, [LName]), Connection) ||
                               [_, LName, _, _, _] <- ?NAMES]),
    sqerl_client:close(Connection).

update_datablob() ->
    Connection = get_db(),
    ?assertMatch({ok, 1},
                 execute(sqerl:statement(update_datablob_by_lname,
					 [<<"foobar">>, "Smith"]), Connection)),
    sqerl_client:close(Connection).

select_datablob() ->
    Connection = get_db(),
    {ok, User} = execute(sqerl:select(find_datablob_by_lname, ["Smith"], first_as_scalar, [datablob]), Connection),
    sqerl_client:close(Connection),
    ?assertMatch(<<"foobar">>, User).

select_boolean() ->
    Connection = get_db(),
    {ok, User} = execute(sqerl:select(find_user_by_lname, ["Smith"], first), Connection),
    ?assertEqual(true, proplists:get_value(<<"active">>, User)),

    {ok, User1} = execute(sqerl:select(find_user_by_lname, ["Presley"], first), Connection),
    ?assertEqual(false, proplists:get_value(<<"active">>, User1)),
    sqerl_client:close(Connection).


%%%
%%% Tests for timestamp behavior....
%%%
update_created() ->
    Connection = get_db(),
    ?assertMatch({ok, 1},
                 execute(sqerl:statement(update_created_by_lname,
					 [{datetime, {{2011, 11, 1}, {16, 47, 46}}},
					  "Smith"]), Connection)),
    ?assertMatch({ok, 1},
                 execute(sqerl:statement(update_created_by_lname,
					 [{{2011, 11, 2}, {16, 47, 46}}, "Anderson"]), Connection)),
    ?assertMatch({ok, 1},
                 execute(sqerl:statement(update_created_by_lname,
					 [<<"2011-11-03 16:47:46">>, "Maier"]), Connection)),

    {ok, User1} = execute(sqerl:select(find_created_by_lname, ["Smith"], first_as_scalar, [created]), Connection),
    ?assertMatch({datetime, {{2011, 11, 01}, {16, 47, 46}}}, User1),
    {ok, User2} = execute(sqerl:select(find_created_by_lname, ["Anderson"], first_as_scalar, [created]), Connection),
    ?assertMatch({datetime, {{2011, 11, 02}, {16, 47, 46}}}, User2),
    {ok, User3} = execute(sqerl:select(find_created_by_lname, ["Maier"], first_as_scalar, [created]), Connection),
    ?assertMatch({datetime, {{2011, 11, 03}, {16, 47, 46}}}, User3),
    sqerl_client:close(Connection).

select_created_by_lname() ->
    Connection = get_db(),
    {ok, User1} = execute(sqerl:select(find_created_by_lname, ["Presley"], first_as_scalar, [created]), Connection),
    ?assertMatch({datetime, {{2011, 10, 04}, {16, 47, 46}}}, User1),
    sqerl_client:close(Connection).
select_lname_by_created() ->
    Connection = get_db(),
    {ok, User1} = execute(sqerl:select(find_lname_by_created, [{datetime, {{2011, 10, 04}, {16, 47, 46}}}], first_as_scalar, [last_name]), Connection),
    ?assertMatch(<<"Presley">>, User1),
    sqerl_client:close(Connection).

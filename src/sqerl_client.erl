%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%% @author Kevin Smith <kevin@opscode.com>
%% @author Omar Yasin <omar@kodi.is>>
%% @doc Abstraction around interacting with SQL databases
%% Copyright 2011-2012 Opscode, Inc. All Rights Reserved.
%% Portions copyright 2012 Omar Yasin. All Rights Reserved.
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

-module(sqerl_client).

-behaviour(gen_server).

-define(LOG_STATEMENT(Name, Args), case application:get_env(sqerl, log_statements) of
                                       undefined ->
                                           ok;
                                       {ok, false} ->
                                           ok;
                                       {ok, true} ->
                                           error_logger:info_msg("(~p) Executing statement ~p with args ~p~n", [self(), Name, Args])
                                   end).

-define(LOG_RESULT(Result), case application:get_env(sqerl, log_statements) of
                                undefined ->
                                    ok;
                                {ok, false} ->
                                    ok;
                                {ok, true} ->
                                    error_logger:info_msg("(~p) Result: ~p~n", [self(), Result])
                            end).

%% API
-export([start_link/6,
         start_link/4,
         exec_prepared_select/3,
         exec_prepared_statement/3,
         close/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% behavior callback
-export([behaviour_info/1]).

-record(state, {cb_mod,
                cb_state,
                timeout}).

%% @hidden
behaviour_info(callbacks) ->
    [{init, 1},
     {exec_prepared_statement, 3},
     {exec_prepared_select, 3},
     {is_connected, 1}];
behaviour_info(_) ->
    undefined.

%%% A select statement returns a list of tuples, or an error.
%%% The prepared statement to use is named by an atom.
-spec exec_prepared_select(pid(), atom(), [any()]) -> [] | [{any(), any()}] | {error, any()}.
exec_prepared_select(Cn, Name, Args) when is_pid(Cn),
                                          is_atom(Name) ->
    gen_server:call(Cn, {exec_prepared_select, Name, Args}, infinity).

%%% Unlike a select statement, this just returns an integer or an error.
-spec exec_prepared_statement(pid(), atom(), []) -> integer() | {error, any()}.
exec_prepared_statement(Cn, Name, Args) when is_pid(Cn),
                                             is_atom(Name) ->
    gen_server:call(Cn, {exec_prepared_stmt, Name, Args}, infinity).


%%% Close a connection
-spec close(pid()) -> ok.
close(Cn) ->
    gen_server:call(Cn, close).

start_link(DbType, Database, Host, Username, Password, Opts) ->
    gen_server:start_link(?MODULE, [DbType, Database, Host, Username, Password, Opts], []).
start_link(DbType, Database, Host, Opts) ->
    gen_server:start_link(?MODULE, [DbType, Database, Host, os:getenv("USER"), "", Opts], []).

init([DbType, Database, Host, Username, Password, Opts]) ->
    CallbackMod = case DbType of
                      pgsql -> sqerl_pgsql_client
                  end,
    IdleCheck = proplists:get_value(idle_check, Opts, 1000),
    Port = proplists:get_value(port, Opts, 5432),
    PreparedStatements = proplists:get_value(prepared_statements, Opts, ev(prepared_statements)),
    ColumnTransforms = proplists:get_value(column_transforms, Opts, ev(column_transforms)),
    Config = [{host, Host},
              {port, Port},
              {user, Username},
              {pass, Password},
              {db, Database},
              {idle_check, IdleCheck},
              {prepared_statements, read_statements(PreparedStatements)},
              {column_transforms, ColumnTransforms}],
    case CallbackMod:init(Config) of
        {ok, CallbackState} ->
            Timeout = IdleCheck,
            {ok, #state{cb_mod=CallbackMod, cb_state=CallbackState,
                        timeout=Timeout}, Timeout};
        Error ->
            {stop, Error}
    end.

handle_call({exec_prepared_select, Name, Args}, _From, #state{cb_mod=CBMod,
                                                              cb_state=CBState,
                                                              timeout=Timeout}=State) ->
    ?LOG_STATEMENT(Name, Args),
    {Result, NewCBState} = CBMod:exec_prepared_select(Name, Args, CBState),
    ?LOG_RESULT(Result),
    {reply, Result, State#state{cb_state=NewCBState}, Timeout};
handle_call({exec_prepared_stmt, Name, Args}, _From, #state{cb_mod=CBMod,
                                                            cb_state=CBState,
                                                            timeout=Timeout}=State) ->
    ?LOG_STATEMENT(Name, Args),
    {Result, NewCBState} = CBMod:exec_prepared_statement(Name, Args, CBState),
    ?LOG_RESULT(Result),
    {reply, Result, State#state{cb_state=NewCBState}, Timeout};
handle_call(close, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, #state{timeout=Timeout}=State) ->
    {reply, ignored, State, Timeout}.

handle_cast(_Msg, #state{timeout=Timeout}=State) ->
    {noreply, State, Timeout}.

handle_info(timeout, #state{cb_mod=CBMod, cb_state=CBState, timeout=Timeout}=State) ->
    case CBMod:is_connected(CBState) of
        {true, CBState1} ->
            {noreply, State#state{cb_state=CBState1}, Timeout};
        false ->
            error_logger:warning_msg("Failed to verify idle connection. Shutting down ~p~n",
                                     [self()]),
            {stop, shutdown, State}
    end;
handle_info(_Info, #state{timeout=Timeout}=State) ->
    {noreply, State, Timeout}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec read_statements([{atom(), term()}]
                      | string()
                      | {atom(), atom(), list()}) -> [{atom(), binary()}].
%% @doc Prepared statements can be provides as a list of `{atom(), binary()}' tuples, as a
%% path to a file that can be consulted for such tuples, or as `{M, F, A}' such that
%% `apply(M, F, A)' returns the statements tuples.
read_statements({Mod, Fun, Args}) ->
    apply(Mod, Fun, Args);
read_statements(L = [{Label, SQL}|_T]) when is_atom(Label) andalso is_binary(SQL) ->
    L;
read_statements(Path) when is_list(Path) ->
    {ok, Statements} = file:consult(Path),
    Statements.

%% Short for "environment value", just provides some sugar for grabbing config values
ev(Key) ->
    case application:get_env(sqerl, Key) of
        {ok, V} -> V;
        undefined ->
            Msg = {missing_application_config, sqerl, Key},
            error_logger:error_report(Msg),
            error(Msg)
    end.

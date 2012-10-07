%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%% @author Seth Falcon <seth@opscode.com>
%% @author Mark Anderson <mark@opscode.com>
%% @author Omar Yasin <omar@kodi.is>
%% Copyright 2011-2012 Opscode, Inc. All Rights Reserved.
%% Portions Copyright 2011-2012 Omar Yasin. All Rights Reserved.
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


-module(sqerl).

-export([select/2,
         select/3,
         select/4,
         statement/2,
         statement/3,
         statement/4]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("sqerl.hrl").

-define(MAX_RETRIES, 5).

%% See http://www.postgresql.org/docs/current/static/errcodes-appendix.html
-define(PGSQL_ERROR_CODES, [{<<"23505">>, conflict}, {<<"23503">>, foreign_key}]).

select(StmtName, StmtArgs) ->
    select(StmtName, StmtArgs, identity, []).

select(StmtName, StmtArgs, {XformName, XformArgs}) ->
    select(StmtName, StmtArgs, XformName, XformArgs);
select(StmtName, StmtArgs, XformName) ->
    select(StmtName, StmtArgs, XformName, []).

select(StmtName, StmtArgs, XformName, XformArgs) ->
    prepare_statement(StmtName, StmtArgs, XformName, XformArgs, exec_prepared_select).

statement(StmtName, StmtArgs) ->
    statement(StmtName, StmtArgs, identity, []).

statement(StmtName, StmtArgs, XformName) ->
    statement(StmtName, StmtArgs, XformName, []).

statement(StmtName, StmtArgs, XformName, XformArgs) ->
    prepare_statement(StmtName, StmtArgs, XformName, XformArgs, exec_prepared_statement).

prepare_statement(StmtName, StmtArgs, XformName, XformArgs, Executor) ->
    Xformer = erlang:apply(sqerl_transformers, XformName, XformArgs),
    F = fun(Cn) ->
                case sqerl_client:Executor(Cn, StmtName, StmtArgs) of
                    {ok, Results} ->
                        Xformer(Results);
                    {error, closed} ->
                        sqerl_client:close(Cn),
                        {error, closed};
                    Error ->
                        parse_error(Error)
                end end,
    F.

%% @doc Utility for generating specific message tuples from database-specific error
%% messages.  The 1-argument form determines which database is being used by querying
%% Sqerl's configuration at runtime, while the 2-argument form takes the database type as a
%% parameter directly.
-spec parse_error(
        {error, {error, error, _, _, _}} %% PostgreSQL error
    ) -> sqerl_error().
parse_error(Reason) ->
    {ok, DbType} = application:get_env(sqerl, db_type),
    parse_error(DbType, Reason).

-spec parse_error(mysql | pgsql, atom() | {term(), term()}
                        | {error, {error, error, _, _, _}}) -> sqerl_error().
parse_error(_DbType, no_connections) ->
    {error, no_connections};
parse_error(_DbType, {no_pool, Type}) ->
    {error, {no_pool, Type}};

parse_error(pgsql, {error,               % error from sqerl
                    {error,              % error record marker from epgsql
                     error,              % Severity
                     Code, Message, _Extra}}) ->
    do_parse_error({Code, Message}, ?PGSQL_ERROR_CODES).

do_parse_error({Code, Message}, CodeList) ->
    case lists:keyfind(Code, 1, CodeList) of
        {_, ErrorType} ->
            {ErrorType, Message};
        false ->
            {error, Message}
    end.

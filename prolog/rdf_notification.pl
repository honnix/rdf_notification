:- module(rdf_notification,
          [
           rdf_subscribe/3,         % +Topic, +Callback, -SubscriberId
           rdf_subscribe/4,         % +Topic, +Callback, +Options, -SubscriberId
           rdf_unsubscribe/1        % +SubscriberId
          ]).

/** <module>  RDF Notification
RDF Notification.

@author Hongxin Liang
@license Apache License Version 2.0
*/

:- use_module(library(option)).
:- use_module(library(gensym)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(thread_pool)).

:- rdf_meta
    rdf_subscribe(r, +, -),
    rdf_subscribe(r, +, +, -).

:- initialization
    thread_create(rdf_notification_main(_{}), _, [alias(rdf_notification_main_thread)]),
    current_prolog_flag(cpu_count, CPUs),
    Size is CPUs + 1,
    thread_pool_create(rdf_notification_caller_thread_pool, Size, []),
    rdf_monitor(rdf_notification:rdf_event_received, [assert,assert(load),retract,update]).

%% rdf_subscribe(+Topic, +Callback, -SubscriberId) is semidet.
%% rdf_subscribe(+Topic, +Callback, +Options, -SubscriberId) is semidet.
%
% Subscribe to the =Topic= which could be =subject=, =predicate= or =object=. When assert,
% retract or update event happens on the topic, =Callback= will be invoked. Upon successful
% subscription, =SubscriberId= will be unified and can be later on used to unsubscribe.
% =Callback= should be prefixed with a module name.
%
% It is highly NOT recommended to do any intensive or long time computation in =Callback=,
% however if that is required, a separated thread must be used.
%
% Supported options are:
%
%  * type(+Type)
%  Can be =subject= (default), =predicate= or =object=, indicating what to subscribe.
%
%  * db(+DB)
%  Specify which database should be monitored. If not specified, all databases will be monitored.
%  Database refers to the fourth argument of rdf_assert/4, and sometimes it's called graph.
%
%  * monitor_descendant(+Boolean)
%  Indicate whether descendant of the =Topic= should also be monitored. The default value is =false=
%  and it's only applicable when =type= is =subject=. For example if =Topic= is =a= and this option is
%  =true=, and there is a triple =rdf(a, b, c)= already in database, =rdf_assert(c, d, e)= will
%  trigger a callback to subscribers of =a=.
%
%  N.B. subscribing to the same =Topic= with the same =Callback= and =Options= will result a new
%  subscribers without having any impact on the existing one. If this is not the wanted result,
%  unsubscribe existing one first.

rdf_subscribe(Topic, Callback, SubscriberId) :-
    rdf_subscribe(Topic, Callback, [type(subject),monitor_descendant(false)], SubscriberId).

rdf_subscribe(Topic, Callback, Options, SubscriberId) :-
    select_option(monitor_descendant(MonitorDescendant), Options, RestOptions1, false),
    select_option(type(Type), RestOptions1, RestOptions2, subject),
    gensym(sub_, SubscriberId),
    thread_send_message(rdf_notification_main_thread,
                        rdf_subscribe(Topic, SubscriberId, Callback,
                                      [type(Type),monitor_descendant(MonitorDescendant)|RestOptions2])
                       ).

%% rdf_unsubscribe(+SubscriberId) is semidet.
%
%  Unsubscribe the subscriber specifed by =SubscriberId=. Unsubscribing a non-existing subscriber
%  does not have any impact.

rdf_unsubscribe(SubscriberId) :-
    thread_send_message(rdf_notification_main_thread,
                        rdf_unsubscribe(SubscriberId)).

%% callback registered to rdf_monitor/2
rdf_event_received(Event) :-
    thread_send_message(rdf_notification_main_thread, Event).

%% main thread handling subscriber, unsubscribe and RDF event
rdf_notification_main(Subscribers) :-
    thread_get_message(Message),
    debug(rdf_notification, 'received message ~w', [Message]),
    handle_message(Subscribers, Message, NewSubscribers),
    rdf_notification_main(NewSubscribers).

handle_message(SubscribersDict, rdf_subscribe(Topic, SubscriberId, Callback, Options),
               NewSubscribersDict) :- !,
    (   Old = SubscribersDict.get(Topic)
    ->  true
    ;   Old = []
    ),
    NewSubscribersDict = SubscribersDict.put(Topic,
                                             [subscriber(SubscriberId, Callback, Options)|Old]),
    debug(rdf_notification, 'SubscribersDict ~w', [NewSubscribersDict]).

handle_message(SubscribersDict, rdf_unsubscribe(SubscriberId), NewSubscribersDict) :-
    get_dict(Topic, SubscribersDict, Old),
    selectchk(subscriber(SubscriberId, _, _), Old, Rest), !,
    (   Rest \= []
    ->  NewSubscribersDict = SubscribersDict.put(Topic, Rest)
    ;   del_dict(Topic, SubscribersDict, _, NewSubscribersDict)
    ),
    debug(rdf_notification, 'SubscribersDict ~w', [NewSubscribersDict]).
handle_message(_, rdf_unsubscribe(_), _) :- !.

handle_message(SubscribersDict, Message, SubscribersDict) :-
    rdf_notify(SubscribersDict, Message).

rdf_notify(SubscribersDict, Event) :-
    Event =.. [_,Subject,Predicate,Object,DB|_],
    rdf_notify_direct_subscribers(SubscribersDict, event_meta(subject, Subject, DB), Event),
    rdf_notify_indirect_subscribers(SubscribersDict, Subject, DB, Event),
    rdf_notify_direct_subscribers(SubscribersDict, event_meta(predicate, Predicate, DB), Event),
    rdf_notify_direct_subscribers(SubscribersDict, event_meta(object, Object, DB), Event).

rdf_notify_direct_subscribers(SubscribersDict, EventMeta, Event) :-
    debug(rdf_notification, 'received event ~w with meta ~w', [Event,EventMeta]),
    rdf_notify_direct_subscribers0(SubscribersDict, EventMeta, Event, false).

rdf_notify_direct_subscribers0(SubscribersDict, event_meta(Type, Topic, DB), Event, FromDescendant) :-
    (   Subscribers = SubscribersDict.get(Topic)
    ->  foreach(select_subscriber(subscriber(SubscriberId, Callback, _),
                                  Subscribers, Type, DB, FromDescendant),
                thread_create_in_pool(rdf_notification_caller_thread_pool,
                                      ignore(call(Callback, SubscriberId, Topic, Event)), _, [])
               )
    ;   true
    ).

select_subscriber(Subscriber, Subscribers, Type, DB, FromDescendant) :-
    member(Subscriber, Subscribers),
    Subscriber = subscriber(_, _, Options),
    option(type(Type), Options),
    (   option(db(DB0), Options)
    ->  DB0 = DB
    ;   true
    ),
    (   FromDescendant
    ->  option(monitor_descendant(true), Options)
    ;   true
    ).

rdf_notify_indirect_subscribers(SubscribersDict, Object, DB, Event) :-
    rdf_notify_indirect_subscribers0(SubscribersDict, Object, DB, Event, [Object], _).

rdf_notify_indirect_subscribers0(SubscribersDict, Object, DB, Event, Seen0, Seen) :-
    findall(Subject,
            (
             rdf(Subject, _, Object), % there might be cross-db reference
             \+ memberchk(Subject, Seen0)
            ),
            Subjects),
    rdf_notify_indirect_subscribers1(SubscribersDict, Subjects, DB, Event, Seen0, Seen).

rdf_notify_indirect_subscribers1(_, [], _, _, Seen, Seen) :- !.
rdf_notify_indirect_subscribers1(SubscribersDict, [H|T], DB, Event, Seen0, Seen) :-
    rdf_notify_direct_subscribers0(SubscribersDict, event_meta(subject, H, DB), Event, true),
    rdf_notify_indirect_subscribers0(SubscribersDict, H, DB, Event, [H|Seen0], Seen1),
    rdf_notify_indirect_subscribers1(SubscribersDict, T, DB, Event, Seen1, Seen).

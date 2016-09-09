:- module(ex2, []).

:- use_module(library(rdf_notification)).
:- use_module(library(semweb/rdf_db)).

:- debug(rdf_notification).
:- debug(ex2).

%% subscribe to subject monitoring descendant
ex2 :-
    rdf_assert(a, b, c),
    rdf_subscribe(a, ex2:callback, [monitor_descendant(true)], SubscriberId),
    debug(ex2, 'subscription confirmed ~w', [SubscriberId]).

callback(SubscriberId, Subject, Event) :-
    debug(ex2, 'received notification ~w of subject ~w, towards ~w',
          [Event,Subject,SubscriberId]),
    rdf_unsubscribe(SubscriberId),
    debug(ex2, 'subscription ended ~w', [SubscriberId]).

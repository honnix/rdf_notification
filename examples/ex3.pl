:- module(ex3, []).

:- use_module(library(rdf_notification)).
:- use_module(library(semweb/rdf_db)).

:- debug(rdf_notification).
:- debug(ex3).

%% subscribe to predicate
ex3 :-
    rdf_subscribe(p1, ex3:callback, [type(predicate)], SubscriberId),
    debug(ex3, 'subscription confirmed ~w', [SubscriberId]).

callback(SubscriberId, Predicate, Event) :-
    debug(ex3, 'received notification ~w of predicate ~w, towards ~w',
          [Event,Predicate,SubscriberId]),
    rdf_unsubscribe(SubscriberId),
    debug(ex3, 'subscription ended ~w', [SubscriberId]).

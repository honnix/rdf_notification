:- module(ex5, []).

:- use_module(library(rdf_notification)).
:- use_module(library(semweb/rdf_db)).

:- debug(rdf_notification).
:- debug(ex5).

%% subscribe to object in specified DB
ex5 :-
    rdf_subscribe(c, ex5:callback, [type(object),db(db1)], SubscriberId),
    debug(ex5, 'subscription confirmed ~w', [SubscriberId]).

callback(SubscriberId, Object, Event) :-
    debug(ex5, 'received notification ~w of object ~w, towards ~w',
          [Event,Object,SubscriberId]),
    rdf_unsubscribe(SubscriberId),
    debug(ex5, 'subscription ended ~w', [SubscriberId]).

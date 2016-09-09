:- module(ex4, []).

:- use_module(library(rdf_notification)).
:- use_module(library(semweb/rdf_db)).

:- debug(rdf_notification).
:- debug(ex4).

%% subscribe to object
ex4 :-
    rdf_subscribe(c, ex4:callback, [type(object)], SubscriberId),
    debug(ex4, 'subscription confirmed ~w', [SubscriberId]).

callback(SubscriberId, Object, Event) :-
    debug(ex4, 'received notification ~w of object ~w, towards ~w',
          [Event,Object,SubscriberId]),
    rdf_unsubscribe(SubscriberId),
    debug(ex4, 'subscription ended ~w', [SubscriberId]).

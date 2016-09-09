:- module(ex1, []).

:- use_module(library(rdf_notification)).

:- debug(rdf_notification).
:- debug(ex1).

%% subscribe to subject without monitoring descendant
ex1 :-
    rdf_subscribe(a, ex1:callback, SubscriberId),
    debug(ex1, 'subscription confirmed ~w', [SubscriberId]).

callback(SubscriberId, Subject, Event) :-
    debug(ex1, 'received notification ~w of subject ~w, towards ~w',
          [Event,Subject,SubscriberId]),
    rdf_unsubscribe(SubscriberId),
    debug(ex1, 'subscription ended ~w', [SubscriberId]).

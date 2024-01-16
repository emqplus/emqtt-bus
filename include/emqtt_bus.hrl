
-include_lib("emqtt/include/emqtt.hrl").

-define(IS_DOMAIN_ID(Id), (is_integer(Id) andalso Id < 255)).


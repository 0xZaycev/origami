-- входящие параметры
local sender_node_id = ARGV[1]; -- инициатор запроса
local request_id = ARGV[2];
local channel = ARGV[3];
local payload = ARGV[4];
local group = ARGV[5];
local no_response = ARGV[6];
local timeout = ARGV[7];



-- всякие ключи
local base_path_key = "origami";
local requests_base_path_key = base_path_key .. ":requests";

local sender_key = base_path_key .. ":clients:" .. sender_node_id;
local sender_pending_pool_key = sender_key .. ":out:pending_pool";

local pending_pool_base_path_key = requests_base_path_key .. ":pending_pool";
local pending_pool_list_key = pending_pool_base_path_key .. ":list";

local request_base_path_key = requests_base_path_key .. ":" .. request_id;
local request_key = request_base_path_key .. ":info";



-- проверяем что ранее не было запроса с таким же ID
local request_is_exists = redis.call("hincrby", request_key, "request_sender_ack", "1");

if not (request_is_exists == 0) then
    -- если данный запрос уже кто-то добавляет, то не надо ему мешать, он сам все сделает
    -- так же, это может быть повторная отправка в случае разрыва соединения
    redis.call('publish', "origami.c" .. sender_node_id, request_id);



    return 1;
end;



-- узнаем текущее время
local time = redis.call("time");
local timestamp = tonumber( time[1] .. "." .. time[2] );

-- статусы:
--      PENDING                 - запрос создан
--      WAIT_REQUEST_ACK        - исполнитель выбран и оповещен, ожидание подтверждения
--      EXECUTING               - исполнитель подтвердил принятие запроса
--      WAIT_RESPONSE_ACK       - ответ был отправлен инициатору, ожидание подтверждения
--      DONE                    - инициатор подтвердил получение ответа

-- сохраняем данные запроса
redis.call('hmset', request_key,
    "sender_node_id", sender_node_id,
    "executor_node_id", "",

    "state", "PENDING",

    "channel", channel,
    "group_key", group,

    "expired", timestamp + tonumber(timeout),
    "timeout", timeout,
    "try_after", (timestamp - 1),
    "no_response", no_response,

    "request_id", request_id,
    "params", payload,
    "response", "",
    "error", "0",

    "request_expired", "-1",
    "request_sender_ack", "0",
    "request_executor_ack", "-1",
    "response_sender_ack", "-1",
    "response_executor_ack", "-1",

    "sent_to_executor_at", "0",
    "executor_accept_at", "0",
    "executor_complete_at", "0",
    "sent_to_initiator_at", "0",
    "initiator_accept_at", "0",

    "created_at", timestamp
);



-- добавим запрос в список отправителя
redis.call("hincrby", sender_key, "out_pending_requests", "1");
redis.call("sadd", sender_pending_pool_key, request_id);

-- добавляем в список ожидания
redis.call("rpush", pending_pool_list_key, request_id);



-- оповещаем иниатора что запрос был получен
redis.call('publish', "origami.c" .. sender_node_id, request_id);



return 1;
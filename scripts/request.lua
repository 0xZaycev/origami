-- входящие параметры
local sender_node_id = ARGV[1]; -- инициатор запроса
local request_id = ARGV[2];
local channel = ARGV[3];
local payload = ARGV[4];



-- всякие ключи
local base_path_key = "origami";
local channels_base_path_key = base_path_key .. ":channels";
local requests_base_path_key = base_path_key .. ":requests";

local sender_key = base_path_key .. ":clients:" .. sender_node_id;
local sender_pending_pool_key = sender_key .. ":out:pending_pool";

local pending_pool_base_path_key = requests_base_path_key .. ":pending_pool";
local pending_pool_list_key = pending_pool_base_path_key .. ":list";
local pending_pool_count_key = pending_pool_base_path_key .. ":count";

local ack_pool_base_path_key = requests_base_path_key .. ":request_ack_pool";
local ack_pool_list_key = ack_pool_base_path_key .. ":list";
local ack_pool_count_key = ack_pool_base_path_key .. ":count";

local request_base_path_key = requests_base_path_key .. ":" .. request_id;
local request_key = request_base_path_key .. ":info";
local request_lock_key = request_base_path_key .. ":request_lock";

local channel_base_path_key = channels_base_path_key .. ":" .. channel;
local channel_listeners_count_key = channel_base_path_key .. ":listeners_count";
local channel_listeners_list_key = channel_base_path_key .. ":listeners_list";



-- проверяем что ранее не было запроса с таким же ID
local request_is_exists = redis.call("incr", request_lock_key);

if not (request_is_exists == 1) then
    -- если данный запрос уже кто-то добавляет, то не надо ему мешать, он сам все сделает

    return 1;
end;



-- узнаем текущее время
local time = redis.call("time");
local timestamp = time[1] .. "." .. time[2];

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
    "request_id", request_id,
    "payload", payload,
    "response", "",
    "error", "",

    "request_ack", "-1",
    "response_ack", "-1",

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



-- пробуем сразу найти исполнитель для этого запроса
local channel_listeners_count = redis.call("incrby", channel_listeners_count_key, "0");

if channel_listeners_count == 0 then
    -- нет исполнителей у канала, кладем запрос в стопку ожидания

    redis.call("lpush", pending_pool_list_key, request_id);
    redis.call("incr", pending_pool_count_key);

    return 1;
end;

-- если список исполнителей не пуст, то подбираем исполнителя
local channel_listeners_list = redis.call('smembers', channel_listeners_list_key);

-- подбирать будем исполнителя с наименьшем кол-вом обрабатываемых в настоящий момент запросов
-- за эталон отсчета будем брать первого активого
local listener_node_id = "";
local listener_pending_requests = 0;



for _, listener in pairs(channel_listeners_list) do
    local listener_client_key = base_path_key .. ":clients:list:" .. listener;
    local listener_active_pool_key = base_path_key .. ":clients:active_pool:" .. listener;



    -- узнаем активен ли исполнитель
    local listener_is_active = redis.call('exists', listener_active_pool_key);

    if listener_is_active == 1 then
        -- исполнитель активен, заполняем переменные

        local _listener_pending_requests = tonumber( redis.call("hget", listener_client_key, "in_pending_requests") );

        if listener_node_id == "" then
            -- это первый исполнитель — просто заполняем переменные

            listener_node_id = listener;
            listener_pending_requests = _listener_pending_requests;
        else
            -- проверим кол-во исполняемых запросов, если меньше, то перепишем переменные

            if _listener_pending_requests < listener_pending_requests then
                listener_node_id = listener;
                listener_pending_requests = _listener_pending_requests;
            end;
        end;
    end;
end;



-- проверим (на всякий случай) если ли исполнитель
if listener_node_id == "" then
    -- исполнителя нет — кладем запрос в стопку ожидания

    redis.call("lpush", pending_pool_list_key, request_id);
    redis.call("incr", pending_pool_count_key);

    return 1;
end;



-- кладем запрос в стопку ожидания подтверждения исполнителя
redis.call('lpush', ack_pool_list_key, request_id);
redis.call("incr", ack_pool_count_key);



-- переменные для работы с клиентом
local listener_key = base_path_key .. ":clients:" .. listener_node_id;
local listener_pending_pool_key = listener_key .. ":in:pending_pool";

-- так же кладем запрос к исполнителю
redis.call("hincrby", listener_key, "in_pending_requests", "1");
redis.call("sadd", listener_pending_pool_key, request_id);



-- устанавляваем исполнителя и меняем статус у запроса
redis.call("hmset", request_key,
    "executor_node_id", listener,

    "state", "WAIT_REQUEST_ACK",

    "sent_to_executor_at", timestamp
);



-- высылаем ивент исполнителю
redis.call("publish", listener .. "." .. channel, request_id .. payload);



return 1;
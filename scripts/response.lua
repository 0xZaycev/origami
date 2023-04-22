-- входящие параметры
local executor_node_id = ARGV[1];
local request_id = ARGV[2];
local is_error = ARGV[3];
local payload = ARGV[4];



-- всякие ключи
local base_path_key = "origami";
local clients_base_path = base_path_key .. ":clients:list";
local channels_base_path_key = base_path_key .. ":channels";
local requests_base_path_key = base_path_key .. ":requests";

local ack_pool_base_path_key = requests_base_path_key .. ":response_ack_pool";
local ack_pool_list_key = ack_pool_base_path_key .. ":list";

local exec_pool_base_path_key = requests_base_path_key .. ":executing_pool";
local exec_pool_list_key = exec_pool_base_path_key .. ":list";

local executor_key = clients_base_path .. ":" .. executor_node_id;
local executor_executing_pool_key = executor_key .. ":in:executing_pool";

local request_base_path_key = requests_base_path_key .. ":" .. request_id;
local request_key = request_base_path_key .. ":info";



-- проверяем что ответ на запрос пришел впервые
local response_is_received = redis.call("hincrby", request_key, "response_executor_ack", "1");

if not (response_is_received == 0) then
    -- ответ уже был получен, просто оповестим исполнителя об этом

    redis.call("publish", "origami.g" .. executor_node_id, request_id);



    return 1;
end;



-- убираем запрос из пула исполнения
redis.call("lrem", exec_pool_list_key, "1", request_id);



-- узнаем текущее время
local time = redis.call("time");
local timestamp = tonumber( time[1] .. "." .. time[2] );



-- обновляем данные запроса
redis.call("hmset", request_key,
    "state", "DONE",

    "response", payload,
    "error", is_error,

    "executor_complete_at", timestamp,
    "sent_to_initiator_at", timestamp,

    "initiator_accept_at", timestamp
);



-- получаем данные запроса
local request_data = redis.call("hmget", request_key,
    "sender_node_id",
    "channel", "group_key",
    "no_response"
);

-- парсим данные
local sender_node_id = request_data[1];
local channel = request_data[2];
local group_key = request_data[3];
local no_response = request_data[4];



-- формируем ключ для запросов к исполнителю
local sender_key = clients_base_path .. ":" .. sender_node_id;
local sender_executing_pool_key = sender_key .. ":out:executing_pool";

-- ключи для работы с каналом
local channel_group_key = channels_base_path_key .. ":" .. channel .. ":groups:" .. group_key;



-- нужно для формирования колонки
local finish_column_name = "processed_requests";

if is_error == "1" then
    finish_column_name = "errored_requests";
end;



-- меняс счетчики инициатора
redis.call("hincrby", sender_key, "out_executing_requests", "-1");
redis.call("hincrby", sender_key, "out_" .. finish_column_name, "1");

-- меняем счетчики исполнителя
redis.call("hincrby", executor_key, "in_executing_requests", "-1");
redis.call("hincrby", executor_key, "in_" .. finish_column_name, "1");



-- убираем запрос из пула исполнения исполнителя
redis.call("srem", executor_executing_pool_key, request_id);

-- убираем запрос из пула исполнения инициатора
redis.call("srem", sender_executing_pool_key, request_id);

-- уменьшаем счетчик группы канала
redis.call("decr", channel_group_key);



-- оповещаем исполнителя о получении его ответа на запрос
redis.call("publish", "origami.g" .. executor_node_id, request_id);



-- проверяем нужен ли инициатору ответ
if no_response == "0" then
    -- если инициатору нужен ответ, то ему его отправить

    -- обновляем данные запроса
    redis.call("hmset", request_key,
        "state", "WAIT_RESPONSE_ACK",
        "initiator_accept_at", "0",
        "try_after", timestamp + 0.1
    );

    -- добавляем в пул ожидания подтверждения получения
    redis.call("rpush", ack_pool_list_key, request_id);

    -- оповещаем инициатора о том, что запрос был исполнен
    redis.call("publish", "origami.e" .. sender_node_id, is_error .. request_id .. executor_node_id .. payload);
end;



-- тик
tick();



return 1;

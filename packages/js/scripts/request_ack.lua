-- входящие аргументы
local executor_node_id = ARGV[1];
local request_id = ARGV[2];



-- всякие ключи
local base_path_key = "origami";
local requests_base_path_key = base_path_key .. ":requests";

local ack_pool_base_path_key = requests_base_path_key .. ":request_ack_pool";
local ack_pool_list_key = ack_pool_base_path_key .. ":list";
local ack_pool_count_key = ack_pool_base_path_key .. ":count";

local exec_pool_base_path_key = requests_base_path_key .. ":executing_pool";
local exec_pool_list_key = exec_pool_base_path_key .. ":list";
local exec_pool_count_key = exec_pool_base_path_key .. ":count";

local request_base_path_key = requests_base_path_key .. ":list:" .. request_id;
local request_key = request_base_path_key .. ":info";

local clients_base_path = base_path_key .. ":clients:list";
local executor_key = clients_base_path .. ":" .. executor_node_id;
local executor_pending_pool_key = executor_key .. ":in:pending_pool";
local executor_executing_pool_key = executor_key .. ":in:executing_pool";



-- проверяем что запрос не взял кто-то другой
local request_is_exists = redis.call("hincrby", request_key, "request_executor_ack", "1");

if not (request_is_exists == 0) then
    -- проверим что этот исполнитель и взял уже запрос
    local request_data = redis.call("hmget", request_key, "state", "executor_node_id");

    local request_state = request_data[1];
    local request_executor = request_data[2];

    -- делаем проверку статуса запроса на случай если
    -- исполнитель получил повторное событие и пытается
    -- снова исполнить запрос
    if (request_state == "WAIT_RESPONSE_ACK" or request_state == "DONE") then
        redis.call("publish", "origami.d" .. executor_node_id, "0" .. request_id);
    end;

    if request_executor == executor_node_id then
        -- это он и есть, прислал повторное подтверждение

        redis.call("publish", "origami.d" .. executor_node_id, "1" .. request_id);
    else
        -- надо уведомить что запрос был уже перераспределен другому исполнителю

        redis.call("publish", "origami.d" .. executor_node_id, "0" .. request_id);
    end;



    return 0;
end;



-- убираем запрос из пула ожидания подтверждения
redis.call("lrem", ack_pool_list_key, "1", request_id);

-- добавляем в пул исполнения
redis.call("rpush", exec_pool_list_key, request_id);



-- узнаем текущее время
local time = redis.call("time");
local timestamp = time[1] .. "." .. time[2];



-- обновляем данные запроса
redis.call("hmset", request_key,
    "executor_node_id", executor_node_id,

    "state", "EXECUTING",

    "executor_accept_at", timestamp
);



-- узнаем ID инициатора запроса
local sender_node_id = redis.call("hget", request_key, "sender_node_id");



-- формируем ключ для запросов к исполнителю
local sender_key = clients_base_path .. ":" .. sender_node_id;
local sender_pending_pool_key = sender_key .. ":out:pending_pool";
local sender_executing_pool_key = sender_key .. ":out:executing_pool";



-- меняс счетчики инициатора
redis.call("hincrby", sender_key, "out_pending_requests", "-1");
redis.call("hincrby", sender_key, "out_executing_requests", "1");

-- меняем счетчики исполнителя
redis.call("hincrby", executor_key, "in_pending_requests", "-1");
redis.call("hincrby", executor_key, "in_executing_requests", "1");



-- перемещаем запрос из пендин пула в пул исполнения
redis.call("smove", sender_pending_pool_key, sender_executing_pool_key, request_id);
redis.call("smove", executor_pending_pool_key, executor_executing_pool_key, request_id);



-- оповещаем исполнителя о том, что он может начать обрабатывать запрос
-- возможно, оповещение можно перенести выше чтобы исполнитель не тратил время
redis.call('publish', "origami.d" .. executor_node_id, "1" .. request_id);



-- тик
tick();



return 1;

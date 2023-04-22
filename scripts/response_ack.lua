-- входящие параметры
local sender_node_id = ARGV[1];
local request_id = ARGV[2];



-- всякие ключи
local base_path_key = "origami";
local requests_base_path_key = base_path_key .. ":requests";

local request_base_path_key = requests_base_path_key .. ":" .. request_id;
local request_key = request_base_path_key .. ":info";

local ack_pool_base_path_key = requests_base_path_key .. ":response_ack_pool";
local ack_pool_list_key = ack_pool_base_path_key .. ":list";



-- узнаем статус запроса
local request_is_exists = redis.call("hincrby", request_key, "response_sender_ack", "1");

if not (request_is_exists == 0) then
    -- инициатор уже подтвердил получение ответа

    redis.call("publish", "origami.f" .. sender_node_id, request_id);



    return 1;
end;



-- убираем запрос из пула подтверждения ответа
redis.call("lrem", ack_pool_list_key, "1", request_id);



-- узнаем текущее время
local time = redis.call("time");
local timestamp = time[1] .. "." .. time[2];



-- обновляем данные запроса
redis.call("hmset", request_key,
    "state", "DONE",

    "initiator_accept_at", timestamp
);

-- устанавляваем время жизни запроса на 3 дня
redis.call("expire", request_key, "259200");


-- оповещаем инициатора
redis.call("publish", "origami.f" .. sender_node_id, request_id);



-- тик
tick();



return 1;
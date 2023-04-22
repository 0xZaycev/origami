-- входящие аргументы
local node_id = ARGV[1];



-- всякие ключи
local base_path_key = "origami";
local client_id_key = base_path_key .. ":clients:list:" .. node_id;

local active_pool_key = base_path_key .. ":clients:active_pool:" .. node_id;



-- узнаем текущее время
local time = redis.call("time");
local timestamp = time[1] .. "." .. time[2];



-- обновляем данные клиента
redis.call("hset", client_id_key, "last_ping_at", timestamp);

-- добавлем слиента в активный пул
redis.call('setex', 10, active_pool_key, timestamp);



-- высылаем клиенту ответ
redis.call("publish", "origami.a" .. node_id, "1");



-- тик
tick();



return 1;
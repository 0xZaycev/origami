-- входящие аргументы
local node_id = ARGV[1];



-- всякие ключи
local base_path_key = "origami";
local client_id_key = base_path_key .. ":clients:list:" .. node_id;

local client_channels_key = client_id_key .. ":channels";
local channel_base_path_key = base_path_key .. ":channels";

local active_pool_key = base_path_key .. ":clients:active_pool:" .. node_id;



-- удаляем клиента из пула активных
redis.call("del", active_pool_key);



-- получаем список подписок клиента
local channels = redis.call("smembers", client_channels_key);

for _, channel in pairs(channels) do
    -- ключ канала
    local channel_listeners_list_key = channel_base_path_key .. ":" .. channel .. ":listeners";

    -- удаляем клиента из слушателей канала
    redis.call("srem", channel_listeners_list_key, node_id);
end;



-- устанавляем всем время жизни 1 неделю
redis.call("expire", client_channels_key, 7 * 24 * 60 * 60);
redis.call("expire", client_id_key .. ":info", 7 * 24 * 60 * 60);



-- высылаем ответ клиенту
redis.call("publish", "origami.h" .. node_id, "1");



return 1;

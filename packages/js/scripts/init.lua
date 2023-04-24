-- входящие аргументы
local node_id = ARGV[1];
local node_name = ARGV[2];

local app_name = ARGV[3];
local app_version = ARGV[4];

local runtime_pid = ARGV[5];
local hostname = ARGV[6];
local platform = ARGV[7];

local client = ARGV[8];
local clientVersion = ARGV[9];

local channels = KEYS;



-- всякие ключи
local base_path_key = "origami";
local client_key = base_path_key .. ":clients:list:" .. node_id;
local client_lock_key = client_key .. ":lock";

local client_info_key = client_key .. ":info";
local client_channels_key = client_key .. ":channels";

local channel_base_path_key = base_path_key .. ":channels";

local active_pool_key = base_path_key .. ":clients:active_pool:" .. node_id;



-- проверяем не создает ли кто-то другой клиента с таким же id (¯\_(ツ)_/¯)
local create_client_lock = redis.call("incr", client_lock_key);

if not (create_client_lock == 1) then
    -- wtf? ( ఠ ͟ʖ ఠ)

    return 0;
end;



-- если уже был клиент с таким id, то мы его перезаписываем

-- узнаем текущее время
local time = redis.call("time");
local timestamp = time[1] .. "." .. time[2];

-- создаем самого клиента
redis.call("hmset", client_info_key,
    "node_id", node_id,
    "node_name", node_name,

    "app_name", app_name,
    "app_version", app_version,

    "runtime_pid", runtime_pid,
    "hostname", hostname,
    "platform", platform,

    "client", client,
    "client_version", clientVersion,

    "in_pending_requests", "0",
    "in_executing_requests", "0",
    "in_errored_requests", "0",
    "in_processed_requests", "0",

    "out_pending_requests", "0",
    "out_executing_requests", "0",
    "out_errored_requests", "0",
    "out_processed_requests", "0",

    "last_ping_at", timestamp,

    "created_at", timestamp
);



-- и чтобы ненароком не накосячить с подписками, мы отпишемся от всех старых подписок клиента
-- и подпишимся на новые


-- получаем список текущих подписок
local old_channels = redis.call("smembers", client_channels_key);

for _, channel_name in pairs(old_channels) do
    -- отписываемся от каждой

    -- убираем клиента из списка доступных обработчиков
    redis.call("srem", channel_base_path_key .. ":" .. channel_name .. ":listeners", node_id);
end;

-- чистим список подписок
redis.call("del", client_channels_key);



-- подписываемся на новые

-- параметры канала
local channel_name = "";
local channel_concurrent = "";
local channel_reservoir_enable = ""; -- 0 - disable; 1 - enable; 2 - by group;
local channel_reservoir_size = "";
local channel_reservoir_interval = "";

for _, channel_parameter in pairs(channels) do
    if channel_name == "" then
        channel_name = channel_parameter .. "";
    else

        if channel_concurrent == "" then
            channel_concurrent = channel_parameter .. "";
        else

            if channel_reservoir_enable == "" then
                channel_reservoir_enable = channel_parameter .. "";
            else

                if channel_reservoir_size == "" then
                    channel_reservoir_size = channel_parameter .. "";
                else
                    channel_reservoir_interval = channel_parameter .. "";



                    local channel_info_key = channel_base_path_key .. ":" .. channel_name .. ":info";
                    local channel_listeners_list_key = channel_base_path_key .. ":" .. channel_name .. ":listeners";

                    -- обновляем данные канала
                    redis.call('hmset', channel_info_key,
                        "name", channel_name,
                        "concurrent", channel_concurrent,
                        "reservoir_enable", channel_reservoir_enable,
                        "reservoir_size", channel_reservoir_size,
                        "reservoir_interval", channel_reservoir_interval,
                        "rr_counter", "0"
                    );

                    -- добавляем клиента в список слушателей канала
                    redis.call("sadd", channel_listeners_list_key, node_id);

                    -- добавляем канал в список каналов клиента
                    redis.call("sadd", client_channels_key, channel_name);



                    -- чистим параметры для следующей итерации
                    channel_name = "";
                    channel_concurrent = "";
                    channel_reservoir_enable = "";
                    channel_reservoir_size = "";
                    channel_reservoir_interval = "";
                end;
            end
        end
    end;
end;



-- добавлем клиента в активный пул
redis.call('setex', active_pool_key, 10, timestamp);

-- снимаем блокировку с клиента
redis.call('del', client_lock_key);



return 1;

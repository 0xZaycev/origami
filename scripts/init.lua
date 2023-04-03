-- входящие аргументы
local node_id = ARGV[1];
local node_name = ARGV[2];

local app_name = ARGV[3];
local app_version = ARGV[4];

local runtime_pid = ARGV[5];
local hostname = ARGV[6];

local channels = KEYS;



-- всякие ключи
local base_path_key = "origami";
local client_id_key = base_path_key .. ":clients:list:" .. node_id;
local client_lock_key = client_id_key .. ":lock";

local client_channels_key = client_id_key .. ":channels";
local client_temp_channels_key = client_channels_key .. ":temp";

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
redis.call("hmset", client_id_key,
    "node_id", node_id,
    "node_name", node_name,

    "app_name", app_name,
    "app_version", app_version,

    "runtime_pid", runtime_pid,
    "hostname", hostname,

    "in_pending_requests", "0",
    "in_executing_requests", "0",
    "in_errored_requests", "0",
    "in_processed_requests", "0",

    "out_pending_requests", "0",
    "out_executing_requests", "0",
    "out_errored_requests", "0",
    "out_processed_requests", "0",

    "last_ping_at", "0",

    "created_at", timestamp
);



-- и чтобы ненароком не накосячить с подписками, надо аккуратно отписаться от созданных ранее

-- добавляем коналы во временный список чтобы потом можно было отписаться от неактивных
redis.call("sadd", client_temp_channels_key, table.unpack(channels));

-- узнаем разницу между старыми каналами и новыми
local diff_channels = redis.call("sdiff", client_channels_key, client_temp_channels_key);

-- параметры канала
local channel_name = "";
local channel_concurrent = "";
local channel_reservoir_enable = ""; -- 0 - disable; 1 - enable; 2 - by group;
local channel_reservoir_size = "";
local channel_reservoir_interval = "";

for _, channel_parameter in pairs(diff_channels) do
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
                        "reservoir_interval", channel_reservoir_interval
                    );

                    -- узнаем является ли клиент слушателем этого канала
                    local channel_is_exists = redis.call('sismember', client_channels_key, channel_name);

                    if channel_is_exists == 1 then
                        -- уже была подписка на канал, надо отписаться

                        -- убираем клиента из списка доступных обработчиков
                        redis.call("srem", channel_listeners_list_key, node_id);

                        -- удираем подписку у клиента
                        redis.call("srem", client_channels_key, channel_name);
                    else
                        -- подписки не было, надо подписаться

                        -- добавляем клиента в список слушателей
                        redis.call("sadd", channel_listeners_list_key, node_id);

                        -- инкрементим кол-во слушаетелей
                        redis.call("incr", channel_listeners_count_key);
                    end;


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



-- временный список больше не нужен — удаляем
redis.call("del", client_temp_channels_key);

-- добавлем слиента в активный пул
redis.call('setex', 10, active_pool_key, timestamp);

-- снимаем блокировку с клиента
redis.call('del', client_lock_key);



return 1;
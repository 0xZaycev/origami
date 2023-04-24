local function tick()
    -- определим базовые ключи
    local base_path_key = "origami";
    local channels_base_path_key = base_path_key .. ":channels";
    local requests_base_path_key = base_path_key .. ":requests";
    local pending_pool_base_path_key = requests_base_path_key .. ":pending_pool";
    local request_ack_pool_base_path_key = requests_base_path_key .. ":request_ack_pool";
    local executing_pool_base_path_key = requests_base_path_key .. ":executing_pool";
    local response_ack_pool_base_path_key = requests_base_path_key .. ":response_ack_pool";

    local tick_lock_key = base_path_key .. ":tick:lock";
    local tick_time_key = base_path_key .. ":tick:time";
    local pending_pool_list_key = pending_pool_base_path_key .. ":list";
    local request_ack_pool_list_key = request_ack_pool_base_path_key .. ":list";
    local executing_pool_list_key = executing_pool_base_path_key .. ":list";
    local response_ack_pool_list_key = response_ack_pool_base_path_key .. ":list";



    -- для начала надо поставить блокировку
    local tick_lock_counter = redis.call("incr", tick_lock_key);

    if tick_lock_counter == 1 then
        -- узнаем текущее время
        local time = redis.call("time");
        local timestamp = tonumber( time[1] .. "." .. time[2] );

        redis.call("set", tick_time_key, timestamp);
    end;

    if not (tick_lock_counter == 2) then
        -- если счетчик блокировки больше одно, значит тик уже выполняется

        return 1;
    end;



    -- узнаем текущее время
    local time = redis.call("time");
    local timestamp = tonumber( time[1] .. "." .. time[2] );



    -- делаем проверку времени чтобы не слишком часто выполнять тик
    local tick_time = tonumber( redis.call("get", tick_time_key) );

    if timestamp <= tick_time then
        -- еще рано выполнять тик, убираем блокировку
        redis.call("set", tick_lock_key, "1");

        return 1;
    end;



    -- получаем кол-во запросов ожидающих подтверждения от исполнителя
    local response_ack_pool_len = redis.call("llen", response_ack_pool_list_key);

    -- итерировать будем по 1000 штук
    local response_ack_pool_loops = math.ceil(response_ack_pool_len / 1000);
    -- так как мы скорее всего будем удалять элементы из списка не только из начала
    -- нам нужен оффсет чтоб не сбиться при получении следующей пачки запросов
    local response_ack_offset = 0;

    for i = 1, response_ack_pool_loops do
        local response_ack_pool_loop = i - 1;
        local list_start = ( response_ack_pool_loop * 1000 ) - response_ack_pool_loop;
        local list_end = list_start + 1000;



        -- забираем нашу пачку запросов
        local requests = redis.call("lrange", response_ack_pool_list_key, list_start, list_end);

        -- начинаем проходиться по массиву
        for _, request_id in pairs(requests) do
            -- срузу формируем нужные нам ключи
            local request_key = requests_base_path_key .. ":list:" .. request_id .. ":info";



            -- получаем данные из запроса
            local request_data = redis.call("hmget", request_key,
                "sender_node_id",
                "try_after",
                "sent_to_initiator_at"
            );

            -- парсим данные запроса
            local sender_node_id = request_data[1];
            local try_after = tonumber( request_data[2] );
            local sent_to_initiator_at = tonumber( request_data[3] );



            -- чтобы не спамить инициатора
            if timestamp > try_after then
                -- мы не будем спамить инициатора вечно, я думаю 2 дней хватит

                -- считаем как давно ответ был создан
                local response_time_delta = timestamp - sent_to_initiator_at;

                if response_time_delta > (2 * 24 * 60 * 60) then
                    -- если прошло более 2 дней, то убираем запрос

                    -- удалем из списка ожидания подтверждения ответа
                    redis.call("lrem", response_ack_pool_list_key, "1", request_id);

                    -- ставим время жизни запросу 6 часов
                    redis.call("expire", request_key, 6 * 60 * 60);

                    -- делаем сдвиг
                    response_ack_offset = response_ack_offset + 1;
                else
                    -- уведоиляем инициатора
                    redis.call("publish", "origami.e" .. sender_node_id, request_id);

                    -- обновляем данные запроса
                    redis.call("hset", request_key, "try_after", timestamp + 0.1);
                end;
            end;
        end;
    end;



    -- получаем кол-во запросов ожидающих подтверждения от исполнителя
    local request_ack_pool_len = redis.call("llen", request_ack_pool_list_key);

    -- итерировать будем по 1000 штук
    local request_ack_pool_loops = math.ceil(request_ack_pool_len / 1000);
    -- так как мы скорее всего будем удалять элементы из списка не только из начала
    -- нам нужен оффсет чтоб не сбиться при получении следующей пачки запросов
    local request_ack_offset = 0;

    for i = 1, request_ack_pool_loops do
        local request_ack_pool_loop = i - 1;
        local list_start = ( request_ack_pool_loop * 1000 ) - request_ack_pool_loop;
        local list_end = list_start + 1000;



        -- забираем нашу пачку запросов
        local requests = redis.call("lrange", request_ack_pool_list_key, list_start, list_end);

        -- начинаем проходиться по массиву
        for _, request_id in pairs(requests) do
            -- срузу формируем нужные нам ключи
            local request_key = requests_base_path_key .. ":list:" .. request_id .. ":info";



            -- получаем данные из запроса
            local request_data = redis.call("hmget", request_key,
                "executor_node_id",
                "channel", "group_key",
                "try_after",
                "sent_to_executor_at"
            );

            -- парсим данные запроса
            local executor_node_id = request_data[1];
            local channel = request_data[2];
            local group_key = request_data[3];
            local try_after = tonumber( request_data[4] );
            local sent_to_executor_at = tonumber( request_data[5] );



            -- ключи, которые мы не могли сформировать без данных запроса
            local channel_group_key = channels_base_path_key .. ":" .. channel .. ":groups:" .. group_key;

            local executor_base_key = base_path_key .. ":clients:list:" .. executor_node_id;
            local executor_client_key = executor_base_key .. ":info";
            local executor_active_pool_key = base_path_key .. ":clients:active_pool:" .. executor_node_id;

            local executor_pending_pool_key = executor_base_key .. ":in:pending_pool";



            -- вычисляем как давно был отправлен первый запрос исполнителю
            local sent_time_delta = timestamp - sent_to_executor_at;

            if sent_time_delta > 5 then
                -- если запрос был отправле более 5 секунд назад
                -- и за это время исполнитель так и не подтвердил его
                -- принятие, то возвращаем в стопку к остальным на
                -- перераспределение



                -- для начала пробуем его занять чтоб его никто другой не занял
                local try_accept_request = redis.call("hincrby", request_key, "request_executor_ack", "1");

                if try_accept_request == 0 then
                    -- запрос был успешно занят



                    -- обновляем данные запроса
                    redis.call("hmset", request_key,
                        "executor_node_id", "",
                        "state", "PENDING",
                        "try_after", timestamp - 1,
                        "sent_to_executor_at", "0"
                    );

                    -- удаляем из текущего списка
                    redis.call("lrem", request_ack_pool_list_key, "1", request_id);

                    -- кладем запрос в начало pending pool
                    redis.call("lpush", pending_pool_list_key, request_id);

                    -- декрементим кол-во активных запросов в группе
                    redis.call("decr", channel_group_key);

                    -- убираем запрос у исполнителя
                    redis.call("srem", executor_pending_pool_key, request_id);

                    -- обновляем данные исполнителя
                    redis.call("hincrby", executor_client_key, "in_pending_requests", "-1");

                    -- и делаем исполнителя неактивным
                    redis.call("del", executor_active_pool_key);

                    -- так же сдвигаем оффсет
                    request_ack_offset = request_ack_offset + 1;
                else
                    -- если кто-то занял запрос прям только что, то хз
                    -- стоит ли делать сдвиг оффсета или нет

                    -- удаляем из текущего списка, если удалим дважды, то ничего страшного
                    redis.call("lrem", request_ack_pool_list_key, "1", request_id);

                    -- так же сдвигаем оффсет
                    request_ack_offset = request_ack_offset + 1;
                end;
            else
                -- если с момента запроса еще не прошло 5 секунд, то
                -- надо попробовать повторно отправить исполнителю ивент

                if timestamp > try_after then
                    -- ну и чтобы не сильно спамить исполнителя, то проверяем
                    -- что прошло достаточно времени перед повторной отправкой

                    redis.call("publish", "origami.b" .. executor_node_id .. channel, request_id);
                end;
            end;
        end;
    end;



    -- в тике мы сначала попробуем раскидать те запросы, которые
    -- только пришли и лежат в pending пуле

    local pending_pool_len = redis.call("llen", pending_pool_list_key);

    -- итерировать будем пачками по 1000 штук
    local pending_pool_loops = math.ceil(pending_pool_len / 1000);
    -- так как мы будем удалять элементы из списка, то нам надо будет
    -- менять индекс
    local pending_pool_offset = 0;

    for i = 1, pending_pool_loops do
        local pending_pool_loop = i - 1;
        local list_start = ( pending_pool_loop * 1000 ) - pending_pool_offset;
        local list_end = list_start + 1000;



        -- забираем нашу пачку запросов
        local pending_requests = redis.call("lrange", pending_pool_list_key, list_start, list_end);

        -- начинаем проходиться по полученному массиву
        for _, request_id in pairs(pending_requests) do
            -- срузу формируем нужные нам ключи
            local request_key = requests_base_path_key .. ":list:" .. request_id .. ":info";

            -- получаем данные из запроса
            local request_data = redis.call("hmget", request_key,
                "sender_node_id", "executor_node_id",
                "channel", "group_key",
                "expired", "timeout", "try_after", "no_response"
            );

            -- парсим данные запроса
            local sender_node_id = request_data[1];
            local executor_node_id = request_data[2];
            local channel = request_data[3];
            local group_key = request_data[4];
            local expired = tonumber( request_data[5] );
            local timeout = tonumber( request_data[6] );
            local try_after = tonumber( request_data[7] );
            local no_response = request_data[8];



            -- ключи, которые мы не могли сформировать без данных запроса
            local channel_info_key = channels_base_path_key .. ":" .. channel .. ":info";
            local channel_groups_key = channels_base_path_key .. ":" .. channel .. ":groups";
            local channel_listeners_list_key = channels_base_path_key .. ":" .. channel .. ":listeners";

            local sender_base_key = base_path_key .. ":clients:list:" .. sender_node_id;
            local sender_client_key = sender_base_key .. ":info";
            local executor_base_key = base_path_key .. ":clients:list:" .. executor_node_id;
            local executor_client_key = executor_base_key .. ":info";

            local sender_pending_pool_key = sender_base_key .. ":out:pending_pool";
            local executor_pending_pool_key = executor_base_key .. ":in:pending_pool";



             -- проверяем не истекло ли время ожидания запроса
            if expired < timestamp and not (timeout == 0) then
                -- время ожидания истекло, пробуем закрыть запрос

                local try_ack_request = redis.call("hincrby", request_key, "request_executor_ack", "1");

                if try_ack_request == 0 then
                    -- запрос был помечен как якобы "принятый другим исполнителем"
                    -- это чтобы его вдруг кто не взял (вряд ли такое может произойти)



                    -- меняем счетчики инициатора и исполнителя
                    redis.call("hincrby", sender_client_key, "out_errored_requests", "1");
                    redis.call("hincrby", sender_client_key, "out_pending_requests", "-1");

                    -- нет ничего плохого в том, что исполнитель не взял запрос, не будем приписывать ему ошибку
                    redis.call('hincrby', executor_client_key, "in_pending_requests", "-1");



                    -- так же, надо переместить из списков исполнителя и инициатора
                    -- в другие списки исполнителя и инициатора
                    redis.call("srem", sender_pending_pool_key, request_id);
                    redis.call("srem", executor_pending_pool_key, request_id);



                    -- удаляем запрос из списка
                    redis.call("lrem", pending_pool_list_key, "1", request_id);

                    -- и помечаем сдвиг чтобы не проебаться на следующем лупе
                    pending_pool_offset = pending_pool_offset + 1;



                    -- обновим данные в запросе
                    redis.call("hmset", request_key,
                        "executor_node_id", "",
                        "state", "DONE",
                        "error", "2", -- 0 - без ошибок; 1 - непредвиденная ошибка; 2 - время ожидания превышено;
                        "request_expired", "0",
                        "executor_complete_at", timestamp,
                        "sent_to_initiator_at", timestamp
                    );



                    -- проверяем нужен ли инициатору ответ
                    if no_response == "0" then
                        -- ответ нужен, перемещаем запрос в пул ожидания подтверждения
                        redis.call("rpush", response_ack_pool_list_key, request_id);

                        -- и отправляем ивент инициатору о том, что запрос был закрыт из-за долгой обработки
                        redis.call("publish", "origami.e" .. sender_node_id, request_id);
                    end;
                end;
            else
                -- для начала проверим что запрос можно исполнять
                -- это сделано для уменьшения нагрузки

                if timestamp > try_after then
                    -- ключ к списку слушателей канала

                    -- чтобы выбрать исполнителя берется список всех исполнителей
                    -- среди них берутся активные и из активных исполнителей
                    -- берется тот у кого меньше всего активных исполнений
                    -- то есть — сам разгруженный

                    -- параметры для отбора исполнителя
                    local listener_node_id = "";
                    local listener_active_requests = 0;

                    local available_listeners = {};

                    -- достаем всех исполнителей кто слушает данный канал
                    local channel_listeners = redis.call("smembers", channel_listeners_list_key);

                    for _, listener in pairs(channel_listeners) do
                        -- ключи для работы с исполнителем при поиске
                        local listener_base_key = base_path_key .. ":clients:list:" .. listener;
                        local listener_client_key = listener_base_key .. ":info";
                        local listener_active_pool_key = base_path_key .. ":clients:active_pool:" .. listener;



                        -- проверяем активен ли исполнитель
                        local listener_is_active = redis.call("exists", listener_active_pool_key);

                        if listener_is_active == 1 then
                            -- добавляем слушателя в список
                            table.insert(available_listeners, listener .. "");

                            ---- так как исполнитель активен, то надо получить список активных
                            --local listener_requests = redis.call("hmget", listener_client_key, "in_pending_requests", "in_executing_requests");
                            --
                            ---- суммируем исполняющиеся запросы и пендящиеся
                            --local _listener_active_requests = tonumber(listener_requests[1]) + tonumber(listener_requests[2]);
                            --
                            --if listener_node_id == "" then
                            --    -- если это первый исполнитель, то пишем его как эталон
                            --    listener_node_id = listener .. "";
                            --    listener_active_requests = _listener_active_requests;
                            --else
                            --    -- проверяем подходит ли исполнитель нам
                            --    if _listener_active_requests < listener_active_requests then
                            --        -- если подходит, то записываем его
                            --        listener_node_id = listener .. "";
                            --        listener_active_requests = _listener_active_requests;
                            --    end;
                            --end;
                        end;
                    end;

                    local pending_counters = redis.call("hincrby", channel_info_key, "rr_counter", "1");

                    local listener_index = math.fmod(pending_counters, #(available_listeners)) + 1;

                    listener_node_id = available_listeners[ (listener_index) ] .. "";

                    -- проверяем есть ли исполнитель
                    if listener_node_id == "" then
                        -- исполнитель не найден, на отложить запрос на секунду

                        redis.call("hset", request_key, "try_after", timestamp + 1);
                    else
                        -- исполнитель найден
                        -- формируем ключи для работы с ним
                        local listener_base_key = base_path_key .. ":clients:list:" .. listener_node_id;
                        local listener_client_key = listener_base_key .. ":info";
                        local listener_pending_pool_key = listener_base_key .. ":in:pending_pool";

                        -- ключ для работы с группой запроса
                        local channel_group_key = channel_groups_key .. ":" .. group_key;

                        -- получаем данные канала
                        local channel_concurrent = tonumber( redis.call("hget", channel_info_key, "concurrent") );



                        -- проверяем лимит на кол-во одновременных запросов
                        if channel_concurrent == 0 then
                            -- если 0 — без ограничений

                            -- инкрементим кол-во активных запросов
                            redis.call("incr", channel_group_key);

                            -- устанавливаем у группы самоудаление через день
                            redis.call("expire", channel_group_key, "86400");



                            -- инкрементим счетчик запросов исполнителя
                            redis.call("hincrby", listener_client_key, "in_pending_requests", "1");

                            -- добавляем запрос в его список ожидания
                            redis.call("sadd", listener_pending_pool_key, request_id);

                            -- удаляем запрос из нашего списка
                            redis.call("lrem", pending_pool_list_key, "1", request_id);

                            -- добавляем запрос в пул подтверждения
                            redis.call("rpush", request_ack_pool_list_key, request_id);

                            -- и помечаем сдвиг чтобы не проебаться на следующем лупе
                            pending_pool_offset = pending_pool_offset + 1;

                            -- обновляем данные запроса
                            redis.call("hmset", request_key,
                                "executor_node_id", listener_node_id,
                                "state", "WAIT_REQUEST_ACK",
                                "request_executor_ack", "-1",
                                "sent_to_executor_at", timestamp
                            );



                            -- оповещаем исполнителя о новой задаче
                            redis.call("publish", "origami.b" .. listener_node_id .. channel, request_id);
                        else
                            -- узнаем кол-во активных запросов
                            local channel_active_requests = redis.call("incrby", channel_group_key, "0");

                            if channel_active_requests < channel_concurrent then
                                -- если кол-во активных запросов меньше максимально допустипово
                                -- то асайним на данного исполнителя



                                -- инкрементим кол-во активных запросов
                                redis.call("incr", channel_group_key);

                                -- устанавливаем у группы самоудаление через день
                                redis.call("expire", channel_group_key, "86400");



                                -- обновляем данные запроса
                                redis.call("hmset", request_key,
                                    "executor_node_id", listener_node_id,
                                    "state", "WAIT_REQUEST_ACK",
                                    "try_after", (timestamp + 0.1),
                                    "sent_to_executor_at", timestamp
                                );



                                -- инкрементим счетчик запросов исполнителя
                                redis.call("hincrby", listener_client_key, "in_pending_requests", "1");

                                -- добавляем запрос в его список ожидания
                                redis.call("sadd", listener_pending_pool_key, request_id);



                                -- удаляем запрос из нашего списка
                                redis.call("lrem", pending_pool_list_key, "1", request_id);

                                -- добавляем запрос в пул подтверждения
                                redis.call("rpush", request_ack_pool_list_key, request_id);

                                -- и помечаем сдвиг чтобы не проебаться на следующем лупе
                                pending_pool_offset = pending_pool_offset + 1;



                                -- оповещаем исполнителя о новой задаче
                                redis.call("publish", "origami.b" .. listener_node_id .. channel, request_id);
                            end;
                        end;
                    end;
                end;
            end;
        end;
    end;



    -- выставляем время для следующего тика
    redis.call("set", tick_time_key, timestamp + 0.01);

    -- снимаем блокировку с тика
    redis.call("set", tick_lock_key, "1");



    return 1;
end;

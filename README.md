# origami

**Origami** - это протокол для гарантированного получения запроса/ответа. 
Это достигается за счет подтверждения получения данных от другой стороны и повторной отправки неполученных данных.

## Предистория

Для реализации модели общения между сервисов в своих проектах я истользовал NATS, REDIS и голый TCP. 
Они довольно быстродейственные и легки в использовании, но редко бывали случаи когда происходили микро-разрывы 
соединения между сервисами из-за чего терялся либо запрос, либо ответ. Интересно, что сетевой 
сокет при этом не ловил события разрыва соединения, программа считала что соединение все так же активно и 
сокет дольше продолжал использоваться, но вот все сообщения уходили в никуда...
\
\
Может у этого есть термин и это решается какими-нибудь ретраями, но в моих кейсах ретраи не подходят. И уже написанные 
библиотеки не включали в себя механизмов проверки соединения.

## Краткое описание

Протокол **Origami** использует **Redis** для отправки запросов и получения событий.
\
\
Реализация включает в себя 3 активных подключения к **Redis**

- **Ping Connection** - используется исключитель для отправки запросов на вызов скиптов `init` и `ping`
- **Listener Connection** - используется для подписки на события и только их и слушает
- **Publisher Connection** - используется для отправки запров (__игнорирует ответы__)

### Подключение клиента

При старте клиент генерирует себе уникальный идентификатор (**UUIDv4**), инициирует пул подключений к **Redis**.
После иниализации подключений отправляется запрос на вызов скрипта `init` в котором передается идентификатор 
клиента и массив каналов в которых он хочет случать запросы

### Отправка запроса

Для удобство введем понятия:
- **Producer** (__продюсер__) - тот кто отправляет запрос
- **Consumer** (__консюмер__) - тот кто обрабатывает запрос

Для каждой стороны есть свои скрипты исполнения:
- (**Producer**)`request` который выбирает получателя из доступного пула хранящегося в **Redis** 
после выбора получателя (__консюмер__) и отправляет ему события типа `call`, 
__продюсеру__ отправляется событие типа `request-ack`, которое подтверждает, что запрос 
был принят и обработан в **Redis**
- (**Consumer**)`call-ack` который должен вызвать __консюмер__ после получения события типа `call` и 
дождаться обратного события типа `call-ack` чтобы убедиться что данный запрос уже точно 
закреплен за ним и не будет перераспределен другому
- (**Consumer**)`response` который вызывается после исполнения __консюмером__ запроса, скрипт 
отправляет собитие типа `response` __продюсеру__ c результатом обработки, и отправляет 
событие типа `response-ack` __консюмеру__ чтобы подтвердить получение ответа в **Redis**
- (**Producer**)`response-ack` вызывается __продюсером__ чтобы подтвердить получение ответа, отправляет 
__продюсеру__ событие типа `response-ack`

![Protocol schema](./protocol-schema.png)
__На фоне изображен персонаж Origami Tobiichi. Одной из черт ее характера является настойчивость. Если у нее есть какая-то цель, то она готова сделать всё, что в ее силах, чтобы достичь ее.__

### Проверка соединения

Соединение проверятся в пассивном режиме.
\
\
Клиент каждую секунду отправляет через **Ping Connection** запрос 
на вызов скрипта `ping` и ждем пока ему от **Listener Connection** 
придет событие что пинг был получени. Если в течении секунды после отправки 
событие не приходит, то это считается неожиданным разрывом соединения и 
все текущие соединения разрывается и подключаются заного.
\
\
После восстановления соединения, производиться попытка повторной 
отправки всех повисших запросов. Все запросы помечены уникальным 
идентификатором, поэтому дублирования бояться не стоит
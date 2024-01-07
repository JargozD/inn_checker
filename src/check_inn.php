<?php

$conf = new RdKafka\Conf();

$conf->set('metadata.broker.list', getenv('KAFKA_CONN'));

$conf->set('group.id', getenv('KAFKA_GROUP_NAME'));

$conf->set('auto.offset.reset', 'earliest');

$conf->set('enable.partition.eof', 'true');

$consumer = new RdKafka\KafkaConsumer($conf);

$consumer->subscribe([getenv('KAFKA_TOPIC_NAME')]);

echo "Начало обработки данных" . PHP_EOL;

$count = 0;

while ($msg = $consumer->consume(5000)) {
    switch ($msg->err) {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            if (!empty($msg->payload)) {
                $data = getData($msg->payload);
                if ($data) {
                    saveData($msg->payload, $data);
                    $count++;

                    echo "Данные по ИНН успешно записаны в файл '$msg->payload'.\n";
                }
            }
            break;
        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
            break 2;
        case RD_KAFKA_RESP_ERR__TIMED_OUT:
            echo "Тайм-аут\n";
            break;
        default:
            throw new \Exception($msg->errstr(), $msg->err);
            break;
    }
}

$consumer->close();

echo "Обработано $count сообщений." . PHP_EOL;

/**
 * Получает данные из сервиса Dadata по ИНН юридического лица
 */
function getData($inn)
{
    $opts = [
        'http' => [
            'method' => "GET",
            'header' => "Authorization: Token " . getenv('DADATA_API_KEY') . "\r\n" .
                "Content-Type: application/json\r\n" .
                "Accept: application/json\r\n"
        ]
    ];

    $result = file_get_contents(
        "http://suggestions.dadata.ru/suggestions/api/4_1/rs/findById/party?query=$inn&type=LEGAL",
        false,
        stream_context_create($opts)
    );

    $result = $result ? json_decode($result, true) : [];

    return $result['suggestions'] ?? [];
}

/**
 * Сохраняет данные по ИНН в файл ./inns/{ИНН}.txt
 */
function saveData($inn, $data)
{
    $fileName = __DIR__ . '/../inns/' . $inn . ".txt";
    file_put_contents($fileName, json_encode($data, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT));
}

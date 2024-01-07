<?php

$producer = new \RdKafka\Producer();

if ($producer->addBrokers(getenv('KAFKA_CONN')) < 1) {
    echo "Ошибка добавления брокера\n";
    exit;
}

$topic = $producer->newTopic(getenv('KAFKA_TOPIC_NAME'));

if (!$producer->getMetadata(false, $topic, 2000)) {
    echo "Ошибка получения метаданных, брокер упал?\n";
    exit;
}

if (isset($_GET['inn'])) {
    $topic->produce(RD_KAFKA_PARTITION_UA, 0, $_GET['inn']);
    echo "Сообщение опубликовано\n";
} else {
    echo "Пустой ИНН\n";
}

$producer->flush(-1);

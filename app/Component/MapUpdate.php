<?php

/**
 * Created by PhpStorm.
 * User: Exodus
 * Date: 02.12.2016
 * Time: 22:29
 */

namespace Exodus4D\Socket\Component;

use Exodus4D\Socket\Component\Handler\LogFileHandler;
use Exodus4D\Socket\Component\Formatter\SubscriptionFormatter;
use Exodus4D\Socket\Data\Payload;
use Exodus4D\Socket\Log\Store;
use Ratchet\ConnectionInterface;

class MapUpdate extends AbstractMessageComponent
{

    /**
     * unique name for this component
     * -> should be overwritten in child instances
     * -> is used as "log store" name
     */
    const COMPONENT_NAME                = 'webSock';

    /**
     * log message unknown task name
     */
    const LOG_TEXT_TASK_UNKNOWN         = 'unknown task: %s';

    /**
     * log message for denied subscription attempt. -> character data unknown
     */
    const LOG_TEXT_SUBSCRIBE_DENY       = 'sub. denied for charId: %d';

    /**
     * log message for invalid subscription data
     */
    const LOG_TEXT_SUBSCRIBE_INVALID    = 'sub. data invalid';

    /**
     * log message for subscribe characterId
     */
    const LOG_TEXT_SUBSCRIBE            = 'sub. charId: %s to mapIds: [%s]';

    /**
     * log message unsubscribe characterId
     */
    const LOG_TEXT_UNSUBSCRIBE          = 'unsub. charId: %d from mapIds: [%s]';

    /**
     * log message for map data updated broadcast
     */
    const LOG_TEXT_MAP_UPDATE           = 'update map data, mapId: %d → broadcast to %d connections';

    /**
     * log message for map subscriptions data updated broadcast
     */
    const LOG_TEXT_MAP_SUBSCRIPTIONS    = 'update map subscriptions data, mapId: %d. → broadcast to %d connections';

    /**
     * log message for delete mapId broadcast
     */
    const LOG_TEXT_MAP_DELETE           = 'delete mapId: $d → broadcast to %d connections';

    /**
     * timestamp (ms) from last healthCheck ping
     * -> timestamp received from remote TCP socket
     * @var int|null
     */
    protected $healthCheckToken;

    /**
     * expire time for map access tokens (seconds)
     * @var int
     */
    protected $mapAccessExpireSeconds = 30;

    /**
     * character access tokens for clients
     * -> tokens are unique and expire onSubscribe!
     * [
     *      'charId_1' => [
     *          [
     *              'token' => $characterToken1,
     *              'expire' => $expireTime1,
     *              'characterData' => $characterData1
     *          ],
     *          [
     *              'token' => $characterToken2,
     *              'expire' => $expireTime2,
     *              'characterData' => $characterData1
     *          ]
     *      ],
     *      'charId_2' => [
     *          [
     *              'token' => $characterToken3,
     *              'expire' => $expireTime3,
     *              'characterData' => $characterData2
     *          ]
     *      ]
     * ]
     * @var array
     */
    protected $characterAccessData;

    /**
     * access tokens for clients grouped by mapId
     * -> tokens are unique and expire onSubscribe!
     * @var array
     */
    protected $mapAccessData;

    /**
     * connected characters
     * [
     *      'charId_1' => [
     *          '$conn1->resourceId' => $conn1,
     *          '$conn2->resourceId' => $conn2
     *      ],
     *      'charId_2' => [
     *          '$conn1->resourceId' => $conn1,
     *          '$conn3->resourceId' => $conn3
     *      ]
     * ]
     * @var array
     */
    protected $characters;

    /**
     * valid client connections subscribed to maps
     * [
     *      'mapId_1' => [
     *          'charId_1' => $charId_1,
     *          'charId_2' => $charId_2
     *      ],
     *      'mapId_2' => [
     *          'charId_1' => $charId_1,
     *          'charId_3' => $charId_3
     *      ]
     * ]
     *
     * @var array
     */
    protected $subscriptions;

    /**
     * collection of characterData for valid subscriptions
     * [
     *      'charId_1' => $characterData1,
     *      'charId_2' => $characterData2
     * ]
     *
     * @var array
     */
    protected $characterData;

    /**
     * MapUpdate constructor.
     * @param Store $store
     */
    public function __construct(Store $store)
    {
        parent::__construct($store);

        $this->characterAccessData  = [];
        $this->mapAccessData        = [];
        $this->characters           = [];
        $this->subscriptions        = [];
        $this->characterData        = [];
    }

    /**
     * new client connection
     * @param \Ratchet\ConnectionInterface $conn
     */
    public function onOpen(ConnectionInterface $conn)
    {
        error_log("[WS] onOpen hit rid=" . ($conn->resourceId ?? 'na'));

        $this->wsSendJson($conn, [
            'type' => 'standalone.hello',
            'ok'   => true,
            'ts'   => time(),
        ]);

        error_log("[WS] onOpen hello sent");
        parent::onOpen($conn);
    }

    /**
     * @param \Ratchet\ConnectionInterface $conn
     */
    public function onClose(ConnectionInterface $conn)
    {
        parent::onClose($conn);

        $this->unSubscribeConnection($conn);
    }

    /**
     * @param \Ratchet\ConnectionInterface $conn
     * @param \Exception $e
     */
    public function onError(ConnectionInterface $conn, \Exception $e)
    {
        parent::onError($conn, $e);

        // close connection should trigger the onClose() callback for unSubscribe
        $conn->close();
    }

    /**
     * @param \Ratchet\ConnectionInterface $conn
     * @param string $msg
     */
    public function onMessage(ConnectionInterface $conn, $msg)
    {
        parent::onMessage($conn, $msg);
    }

    /**
     * @param \Ratchet\ConnectionInterface $conn
     * @param Payload $payload
     */
    protected function dispatchWebSocketPayload(ConnectionInterface $conn, Payload $payload): void
    {
        switch ($payload->task) {
            case 'healthCheck':
                $this->broadcastHealthCheck($conn, $payload);
                break;
            case 'standalone.bind':
                $data = (array)$payload->load;

                $ticket = (string)($data['ticket'] ?? '');
                if ($ticket === '') {
                    $this->wsSendJson($conn, [
                        'type' => 'standalone.bound',
                        'ok'   => false,
                        'code' => 'missing_ticket',
                    ]);
                    break;
                }

                $v = $this->standaloneTicketGet($ticket);
                if (!$v['ok']) {
                    $this->wsSendJson($conn, [
                        'type' => 'standalone.bound',
                        'ok'   => false,
                        'code' => $v['code'],
                    ]);
                    break;
                }

                $cid = (int)$v['cid'];

                /**
                 * [재바인딩 방어 초기화]
                 * 같은 WS conn에서 bind가 여러 번 호출되거나,
                 * 이전 heartbeat 주입 캐릭터가 남아있는 상태로 다른 cid로 갈아타는 경우를 방지.
                 *
                 * 이 conn(resourceId)이 "활성 커넥션"으로 등록되어 있던 모든 캐릭터에서 떼고,
                 * subscriptions에도 남아있으면 제거 후 mapSubscriptions 브로드캐스트.
                 */
                $changedMapIds = [];

                // 1) 이 conn이 물고 있던 캐릭터 목록 (메인 포함, 주입 alt 포함)
                $prevCharacterIds = $this->getCharacterIdsByConnection($conn);

                if (!empty($prevCharacterIds)) {
                    // 2) characters[characterId][resourceId] 해제
                    foreach ($prevCharacterIds as $prevCid) {
                        $prevCid = (int)$prevCid;
                        if ($prevCid <= 0) continue;

                        if (isset($this->characters[$prevCid][$conn->resourceId])) {
                            unset($this->characters[$prevCid][$conn->resourceId]);

                            // 이 캐릭터에 더 이상 어떤 커넥션도 없다면 캐릭터 자체 제거 + characterData 제거
                            if (empty($this->characters[$prevCid])) {
                                unset($this->characters[$prevCid]);
                                $this->deleteCharacterData($prevCid);

                                // 3) subscriptions에서 제거 (characterIds는 charId 단위)
                                foreach ($this->subscriptions as $mapId => $subData) {
                                    if (isset($this->subscriptions[$mapId]['characterIds'][$prevCid])) {
                                        unset($this->subscriptions[$mapId]['characterIds'][$prevCid]);

                                        if (!count($this->subscriptions[$mapId]['characterIds'])) {
                                            unset($this->subscriptions[$mapId]);
                                        }

                                        $changedMapIds[] = (int)$mapId;
                                    }
                                }
                            }
                        }
                    }
                }

                // 4) 변경된 맵만 갱신 브로드캐스트
                if (!empty($changedMapIds)) {
                    $changedMapIds = array_values(array_unique($changedMapIds));
                    sort($changedMapIds, SORT_NUMERIC);
                    $this->broadcastMapSubscriptions($changedMapIds);
                }

                // 이제 "새 세션"으로 바인딩
                $conn->standaloneCid = $cid;

                $this->wsSendJson($conn, [
                    'type'     => 'standalone.bound',
                    'ok'       => true,
                    'cid'      => $cid,
                    'ttl'      => (int)$v['ttl'],
                    'serverTs' => time(),
                ]);
                break;
            case 'standalone.heartbeat':
                $data = (array)$payload->load;

                if (!isset($conn->standaloneCid)) {
                    $this->wsSendJson($conn, [
                        'type' => 'standalone.ack',
                        'ok'   => false,
                        'code' => 'not_bound',
                    ]);
                    break;
                }

                // ✅ 주입 대상 맵은 payload에서 받는다
                $mapId = (int)($data['mapId'] ?? 0);
                if ($mapId <= 0) {
                    $this->wsSendJson($conn, [
                        'type' => 'standalone.ack',
                        'ok'   => false,
                        'code' => 'missing_mapId',
                    ]);
                    break;
                }

                // ✅ 만료된 presence 먼저 청소(너가 이미 넣었다고 한 함수)
                // (mapId가 있어야 mapSubscriptions 브로드캐스트도 안전하게 가능)
                $this->cleanupStandaloneExpiredPresence($conn, $mapId);

                // ✅ subscriptions[mapId]가 없으면 standalone이 컨테이너를 만든다
                if (!isset($this->subscriptions[$mapId])) {
                    $this->subscriptions[$mapId] = [
                        'connections'  => [],
                        'characterIds' => [],
                    ];
                }

                // payload chars
                $chars = $data['chars'] ?? [];
                if (!is_array($chars)) $chars = [];

                // incoming: cid => ['id'=>cid,'name'=>name]  (presence-only)
                $incoming = [];
                foreach ($chars as $row) {
                    if (is_object($row)) $row = (array)$row; // stdClass 대응
                    if (!is_array($row)) continue;

                    $cid  = (int)($row['id'] ?? 0);
                    $name = trim((string)($row['name'] ?? ''));

                    if ($cid <= 0 || $name === '') continue;

                    // ✅ presence-only: id/name만 취급 (log/corp/alliance는 받더라도 무시하는 게 안전)
                    $incoming[$cid] = [
                        'id'   => $cid,
                        'name' => $name,
                    ];
                }

                // snapshot diff: 이 conn이 물고 있는 캐릭터들 기준
                $existing = $this->getCharacterIdsByConnection($conn);
                $desired  = array_keys($incoming);

                $toAdd    = array_values(array_diff($desired, $existing));
                $toRemove = array_values(array_diff($existing, $desired));

                // ✅ TTL 갱신(Lease)
                // heartbeat 주기보다 충분히 길게(예: 120초) 잡아야 "잠깐 지연"에 끊기지 않음
                $ttlSec = 120;
                $now = time();
                if (!isset($conn->standalonePresenceExp) || !is_array($conn->standalonePresenceExp)) {
                    $conn->standalonePresenceExp = [];
                }
                foreach ($desired as $cid) {
                    $cid = (int)$cid;
                    if ($cid > 0) $conn->standalonePresenceExp[$cid] = $now + $ttlSec;
                }
                // ✅ PF cron이 읽을 presence 파일 기록
                $dir = '/var/www/html/pathfinder/tmp/pf';
                if (!is_dir($dir)) @mkdir($dir, 0775, true);

                $presence = [
                    'mapId' => $mapId,
                    'ts'    => time(),
                    'ttl'   => 120,               // heartbeat TTL과 동일하게
                    'chars' => array_values($desired),
                ];

                // atomic write
                $tmp = $dir . "/standalone_presence_map_{$mapId}.json.tmp";
                $dst = $dir . "/standalone_presence_map_{$mapId}.json";
                @file_put_contents($tmp, json_encode($presence), LOCK_EX);
                @rename($tmp, $dst);

                // REMOVE: 이번 heartbeat에 없는 캐릭터는 "이 conn"에서만 분리
                foreach ($toRemove as $cid) {
                    $cid = (int)$cid;

                    if (isset($this->characters[$cid][$conn->resourceId])) {
                        unset($this->characters[$cid][$conn->resourceId]);
                    }

                    // 만료 테이블에서도 제거(이번 틱에 없다는 건 "현재 스냅샷에 없다"는 뜻)
                    if (isset($conn->standalonePresenceExp[$cid])) {
                        unset($conn->standalonePresenceExp[$cid]);
                    }

                    // 완전 고아일 때만 해당 mapId 구독자에서 제거
                    if (empty($this->characters[$cid])) {
                        unset($this->characters[$cid]);

                        if (isset($this->subscriptions[$mapId]['characterIds'][$cid])) {
                            unset($this->subscriptions[$mapId]['characterIds'][$cid]);
                        }

                        // ⚠️ presence-only 정책:
                        // characterData 삭제는 하지 않는다. (cron/ESI가 채운 log를 같이 날릴 수 있음)
                        // $this->deleteCharacterData($cid);
                    }
                }

                // ADD: 이번 heartbeat 캐릭터들은 이 conn을 active connection처럼 공유 등록 + mapId 구독자에 올림
                foreach ($toAdd as $cid) {
                    $cid = (int)$cid;

                    // ✅ 10탭 대체 핵심: 하나의 conn을 여러 캐릭터의 "활성 커넥션"으로 공유 등록
                    $this->characters[$cid][$conn->resourceId] = $conn;

                    // mapId 구독자에 추가
                    $this->subscriptions[$mapId]['characterIds'][$cid] = $cid;

                    // ✅ id/name만 갱신 + 기존 log 등은 절대 덮지 않음
                    $this->mergeCharacterPresenceOnly($incoming[$cid]);
                }

                // UPSERT: 이미 있던 애들도 name만 최신 반영(log 보호)
                foreach ($desired as $cid) {
                    $cid = (int)$cid;
                    $this->mergeCharacterPresenceOnly($incoming[$cid]);

                    // mapId 구독자에 계속 유지(혹시 빠져있으면 복구)
                    $this->subscriptions[$mapId]['characterIds'][$cid] = $cid;

                    // characters 연결 유지(혹시 빠져있으면 복구)
                    if (!isset($this->characters[$cid][$conn->resourceId])) {
                        $this->characters[$cid][$conn->resourceId] = $conn;
                    }
                }

                // 해당 맵만 subscriptions 브로드캐스트
                $this->broadcastMapSubscriptions([$mapId]);

                $this->wsSendJson($conn, [
                    'type'     => 'standalone.ack',
                    'ok'       => true,
                    'cid'      => (int)$conn->standaloneCid,
                    'mapId'    => $mapId,
                    'received' => count($desired),
                    'add'      => count($toAdd),
                    'remove'   => count($toRemove),
                    'maps'     => 1,
                    'serverTs' => time(),
                ]);
                break;
            case 'subscribe':
                $this->subscribe($conn, (array)$payload->load);
                break;
            case 'unsubscribe':
                // make sure characterIds got from client are valid
                // -> intersect with subscribed characterIds for current $conn
                $characterIds = array_intersect((array)$payload->load, $this->getCharacterIdsByConnection($conn));
                if (!empty($characterIds)) {
                    $this->unSubscribeCharacterIds($characterIds, $conn);
                }
                break;
            default:
                $this->log(['debug', 'error'], $conn, __FUNCTION__, sprintf(static::LOG_TEXT_TASK_UNKNOWN, $payload->task));
                break;
        }
    }

    /**
     * checks healthCheck $token and respond with validation status + subscription stats
     * @param \Ratchet\ConnectionInterface $conn
     * @param Payload $payload
     */
    private function broadcastHealthCheck(ConnectionInterface $conn, Payload $payload): void
    {
        $isValid = $this->validateHealthCheckToken((int)$payload->load);

        $load = [
            'isValid' => $isValid,
        ];

        // Make sure WebSocket client request is valid
        if ($isValid) {
            // set new healthCheckToken for next check
            $load['token'] = $this->setHealthCheckToken(microtime(true));

            // add subscription stats if $token is valid
            $load['subStats'] = $this->getSubscriptionStats();
        }

        $payload->setLoad($load);

        $connections = new \SplObjectStorage();
        $connections->attach($conn);

        $this->broadcast($connections, $payload);
    }

    /**
     * compare token (timestamp from initial TCP healthCheck message) with token send from WebSocket
     * @param int $token
     * @return bool
     */
    private function validateHealthCheckToken(int $token): bool
    {
        $isValid = false;

        if ($token && $this->healthCheckToken && $token === (int)$this->healthCheckToken) {
            $isValid = true;
        }

        // reset token
        $this->healthCheckToken = null;

        return $isValid;
    }

    /**
     * subscribes a connection to valid accessible maps
     * @param \Ratchet\ConnectionInterface $conn
     * @param $subscribeData
     */
    private function subscribe(ConnectionInterface $conn, array $subscribeData): void
    {
        $characterId = (int)$subscribeData['id'];
        $characterToken = (string)$subscribeData['token'];

        if ($characterId && $characterToken) {
            // check if character access token is valid (exists and not expired in $this->characterAccessData)
            if ($characterData = $this->checkCharacterAccess($characterId, $characterToken)) {
                $this->characters[$characterId][$conn->resourceId] = $conn;

                // insert/update characterData cache
                // -> even if characterId does not have access to a map "yet"
                // -> no maps found but character can get map access at any time later
                $this->setCharacterData($characterData);

                // valid character -> check map access
                $changedSubscriptionsMapIds = [];
                foreach ((array)$subscribeData['mapData'] as $data) {
                    $mapId = (int)$data['id'];
                    $mapToken = (string)$data['token'];
                    $mapName = (string)$data['name'];

                    if ($mapId && $mapToken) {
                        // check if token is valid (exists and not expired) in $this->mapAccessData
                        if ($this->checkMapAccess($characterId, $mapId, $mapToken)) {
                            // valid map subscribe request
                            $this->subscriptions[$mapId]['characterIds'][$characterId] = $characterId;
                            $this->subscriptions[$mapId]['data']['name'] = $mapName;
                            $changedSubscriptionsMapIds[] = $mapId;
                        }
                    }
                }

                sort($changedSubscriptionsMapIds, SORT_NUMERIC);

                $this->log(
                    ['debug', 'info'],
                    $conn,
                    __FUNCTION__,
                    sprintf(static::LOG_TEXT_SUBSCRIBE, $characterId, implode(',', $changedSubscriptionsMapIds))
                );

                // broadcast all active subscriptions to subscribed connections -------------------------------------------
                $this->broadcastMapSubscriptions($changedSubscriptionsMapIds);
            } else {
                $this->log(['debug', 'info'], $conn, __FUNCTION__, sprintf(static::LOG_TEXT_SUBSCRIBE_DENY, $characterId));
            }
        } else {
            $this->log(['debug', 'error'], $conn, __FUNCTION__, static::LOG_TEXT_SUBSCRIBE_INVALID);
        }
    }

    /**
     * subscribes an active connection from maps
     * @param \Ratchet\ConnectionInterface $conn
     */
    private function unSubscribeConnection(ConnectionInterface $conn)
    {
        $characterIds = $this->getCharacterIdsByConnection($conn);
        $this->unSubscribeCharacterIds($characterIds, $conn);
    }

    /**
     * unSubscribe a $characterId from ALL maps
     * -> if $conn is set -> just unSub the $characterId from this $conn
     * @param int $characterId
     * @param \Ratchet\ConnectionInterface|null $conn
     * @return bool
     */
    private function unSubscribeCharacterId(int $characterId, ?ConnectionInterface $conn = null): bool
    {
        if ($characterId) {
            // unSub from $this->characters ---------------------------------------------------------------------------
            if ($conn) {
                // just unSub a specific connection (e.g. single browser window)
                unset($this->characters[$characterId][$conn->resourceId]);

                if (!count($this->characters[$characterId])) {
                    // no connection left for this character
                    unset($this->characters[$characterId]);
                }
                // TODO unset $this->>$characterData if $characterId does not have any other map subscribed to
            } else {
                // unSub ALL connections from a character (e.g. multiple browsers)
                unset($this->characters[$characterId]);

                // unset characterData cache
                $this->deleteCharacterData($characterId);
            }

            // unSub from $this->subscriptions ------------------------------------------------------------------------
            $changedSubscriptionsMapIds = [];
            foreach ($this->subscriptions as $mapId => $subData) {
                if (array_key_exists($characterId, (array)$subData['characterIds'])) {
                    unset($this->subscriptions[$mapId]['characterIds'][$characterId]);

                    if (!count($this->subscriptions[$mapId]['characterIds'])) {
                        // no characters left on this map
                        unset($this->subscriptions[$mapId]);
                    }

                    $changedSubscriptionsMapIds[] = $mapId;
                }
            }

            sort($changedSubscriptionsMapIds, SORT_NUMERIC);

            $this->log(
                ['debug', 'info'],
                $conn,
                __FUNCTION__,
                sprintf(static::LOG_TEXT_UNSUBSCRIBE, $characterId, implode(',', $changedSubscriptionsMapIds))
            );

            // broadcast all active subscriptions to subscribed connections -------------------------------------------
            $this->broadcastMapSubscriptions($changedSubscriptionsMapIds);
        }

        return true;
    }

    /**
     * unSubscribe $characterIds from ALL maps
     * -> if $conn is set -> just unSub the $characterId from this $conn
     * @param int[] $characterIds
     * @param \Ratchet\ConnectionInterface|null $conn
     * @return bool
     */
    private function unSubscribeCharacterIds(array $characterIds, ?ConnectionInterface $conn = null): bool
    {
        $response = false;
        foreach ($characterIds as $characterId) {
            $response = $this->unSubscribeCharacterId($characterId, $conn);
        }
        return $response;
    }

    /**
     * delete mapId from subscriptions and broadcast "delete msg" to clients
     * @param string $task
     * @param int $mapId
     * @return int
     */
    private function deleteMapId(string $task, int $mapId): int
    {
        $connectionCount = $this->broadcastMapData($task, $mapId, $mapId);

        // remove map from subscriptions
        if (isset($this->subscriptions[$mapId])) {
            unset($this->subscriptions[$mapId]);
        }

        $this->log(
            ['debug', 'info'],
            null,
            __FUNCTION__,
            sprintf(static::LOG_TEXT_MAP_DELETE, $mapId, $connectionCount)
        );

        return $connectionCount;
    }

    /**
     * get all mapIds a characterId has subscribed to
     * @param int $characterId
     * @return int[]
     */
    private function getMapIdsByCharacterId(int $characterId): array
    {
        $mapIds = [];
        foreach ($this->subscriptions as $mapId => $subData) {
            if (array_key_exists($characterId, (array)$subData['characterIds'])) {
                $mapIds[] = $mapId;
            }
        }
        return $mapIds;
    }

    /**
     * @param \Ratchet\ConnectionInterface $conn
     * @return int[]
     */
    private function getCharacterIdsByConnection(ConnectionInterface $conn): array
    {
        $characterIds = [];
        $resourceId = $conn->resourceId;

        foreach ($this->characters as $characterId => $resourceIDs) {
            if (
                array_key_exists($resourceId, $resourceIDs) &&
                !in_array($characterId, $characterIds)
            ) {
                $characterIds[] = $characterId;
            }
        }
        return $characterIds;
    }

    /**
     * @param $mapId
     * @return array
     */
    private function getCharacterIdsByMapId(int $mapId): array
    {
        $characterIds = [];
        if (
            array_key_exists($mapId, $this->subscriptions) &&
            is_array($this->subscriptions[$mapId]['characterIds'])
        ) {
            $characterIds = array_keys($this->subscriptions[$mapId]['characterIds']);
        }
        return $characterIds;
    }

    /**
     * get connections by $characterIds
     * @param int[] $characterIds
     * @return \SplObjectStorage
     */
    private function getConnectionsByCharacterIds(array $characterIds): \SplObjectStorage
    {
        $connections = new \SplObjectStorage;
        foreach ($characterIds as $characterId) {
            $connections->addAll($this->getConnectionsByCharacterId($characterId));
        }
        return $connections;
    }

    /**
     * get connections by $characterId
     * @param int $characterId
     * @return \SplObjectStorage
     */
    private function getConnectionsByCharacterId(int $characterId): \SplObjectStorage
    {
        $connections = new \SplObjectStorage;
        if (isset($this->characters[$characterId])) {
            foreach (array_keys($this->characters[$characterId]) as $resourceId) {
                if (
                    $this->hasConnectionId($resourceId) &&
                    !$connections->contains($conn = $this->getConnection($resourceId))
                ) {
                    $connections->attach($conn);
                }
            }
        }
        return $connections;
    }

    /**
     * check character access against $this->characterAccessData whitelist
     * @param $characterId
     * @param $characterToken
     * @return array
     */
    private function checkCharacterAccess(int $characterId, string $characterToken): array
    {
        $characterData = [];
        if (!empty($characterAccessData = (array)$this->characterAccessData[$characterId])) {
            // check expire for $this->characterAccessData -> check ALL characters and remove expired
            foreach ($characterAccessData as $i => $data) {
                $deleteToken = false;

                if (((int)$data['expire'] - time()) > 0) {
                    // still valid -> check token
                    if ($characterToken === $data['token']) {
                        $characterData = $data['characterData'];
                        $deleteToken = true;
                        // NO break; here -> check other characterAccessData as well
                    }
                } else {
                    // token expired
                    $deleteToken = true;
                }

                if ($deleteToken) {
                    unset($this->characterAccessData[$characterId][$i]);
                    // -> check if tokens for this charId is empty
                    if (empty($this->characterAccessData[$characterId])) {
                        unset($this->characterAccessData[$characterId]);
                    }
                }
            }
        }

        return $characterData;
    }

    /**
     * check map access against $this->mapAccessData whitelist
     * @param $characterId
     * @param $mapId
     * @param $mapToken
     * @return bool
     */
    private function checkMapAccess(int $characterId, int $mapId, string $mapToken): bool
    {
        $access = false;
        if (!empty($mapAccessData = (array)$this->mapAccessData[$mapId][$characterId])) {
            foreach ($mapAccessData as $i => $data) {
                $deleteToken = false;
                // check expire for $this->mapAccessData -> check ALL characters and remove expired
                if (((int)$data['expire'] - time()) > 0) {
                    // still valid -> check token
                    if ($mapToken === $data['token']) {
                        $access = true;
                        $deleteToken = true;
                    }
                } else {
                    // token expired
                    $deleteToken = true;
                }

                if ($deleteToken) {
                    unset($this->mapAccessData[$mapId][$characterId][$i]);
                    // -> check if tokens for this charId is empty
                    if (empty($this->mapAccessData[$mapId][$characterId])) {
                        unset($this->mapAccessData[$mapId][$characterId]);
                        // -> check if map has no access tokens left for characters
                        if (empty($this->mapAccessData[$mapId])) {
                            unset($this->mapAccessData[$mapId]);
                        }
                    }
                }
            }
        }
        return $access;
    }

    /**
     * broadcast $payload to $connections
     * @param \SplObjectStorage $connections
     * @param Payload $payload
     */
    private function broadcast(\SplObjectStorage $connections, Payload $payload): void
    {
        $data = json_encode($payload);
        foreach ($connections as $conn) {
            $this->send($conn, $data);
        }
    }

    // custom calls ===================================================================================================

    /**
     * receive data from TCP socket (main App)
     * -> send response back
     * @param string $task
     * @param null|int|array $load
     * @return bool|float|int|null
     */
    public function receiveData(string $task, $load = null)
    {
        $responseLoad = null;

        switch ($task) {
            case 'healthCheck':
                $responseLoad = $this->setHealthCheckToken((float)$load);
                break;
            case 'characterUpdate':
                $this->updateCharacterData((array)$load);
                $mapIds = $this->getMapIdsByCharacterId((int)$load['id']);
                $this->broadcastMapSubscriptions($mapIds);
                break;
            case 'characterLogout':
                $responseLoad = $this->unSubscribeCharacterIds((array)$load);
                break;
            case 'mapConnectionAccess':
                $responseLoad = $this->setConnectionAccess($load);
                break;
            case 'mapAccess':
                $responseLoad = $this->setAccess($task, $load);
                break;
            case 'mapUpdate':
                $responseLoad = $this->broadcastMapUpdate($task, (array)$load);
                break;
            case 'mapDeleted':
                $responseLoad = $this->deleteMapId($task, (int)$load);
                break;
            case 'logData':
                $this->handleLogData((array)$load['meta'], (array)$load['log']);
                break;
        }

        return $responseLoad;
    }

    /**
     * @param float $token
     * @return float
     */
    private function setHealthCheckToken(float $token): float
    {
        $this->healthCheckToken = $token;
        return $this->healthCheckToken;
    }

    /**
     * @param array $characterData
     */
    private function setCharacterData(array $characterData): void
    {
        if ($characterId = (int)$characterData['id']) {
            $this->characterData[$characterId] = $characterData;
        }
    }

    /**
     * @param int $characterId
     * @return array
     */
    private function getCharacterData(int $characterId): array
    {
        return empty($this->characterData[$characterId]) ? [] : $this->characterData[$characterId];
    }

    /**
     * @param array $characterIds
     * @return array
     */
    private function getCharactersData(array $characterIds): array
    {
        return array_filter($this->characterData, function ($characterId) use ($characterIds) {
            return in_array($characterId, $characterIds);
        }, ARRAY_FILTER_USE_KEY);
    }

    /**
     * @param array $characterData
     */
    private function updateCharacterData(array $characterData): void
    {
        $characterId = (int)($characterData['id'] ?? 0);
        if ($characterId <= 0) return;
        $this->setCharacterData($characterData);
    }

    /**
     * @param int $characterId
     */
    private function deleteCharacterData(int $characterId): void
    {
        unset($this->characterData[$characterId]);
    }

    /**
     * @param array $mapIds
     */
    private function broadcastMapSubscriptions(array $mapIds): void
    {
        $mapIds = array_unique($mapIds);

        foreach ($mapIds as $mapId) {
            if (
                !empty($characterIds = $this->getCharacterIdsByMapId($mapId)) &&
                !empty($charactersData = $this->getCharactersData($characterIds))
            ) {
                $systems = SubscriptionFormatter::groupCharactersDataBySystem($charactersData);

                $mapUserData = (object)[];
                $mapUserData->config = (object)['id' => $mapId];
                $mapUserData->data = (object)['systems' => $systems];

                $connectionCount = $this->broadcastMapData('mapSubscriptions', $mapId, $mapUserData);

                $this->log(
                    ['debug'],
                    null,
                    __FUNCTION__,
                    sprintf(static::LOG_TEXT_MAP_SUBSCRIPTIONS, $mapId, $connectionCount)
                );
            }
        }
    }

    /**
     * @param string $task
     * @param array $mapData
     * @return int
     */
    private function broadcastMapUpdate(string $task, array $mapData): int
    {
        $mapId = (int)$mapData['config']['id'];
        $connectionCount =  $this->broadcastMapData($task, $mapId, $mapData);

        $this->log(
            ['debug'],
            null,
            __FUNCTION__,
            sprintf(static::LOG_TEXT_MAP_UPDATE, $mapId, $connectionCount)
        );

        return $connectionCount;
    }

    /**
     * send map data to ALL connected clients
     * @param string $task
     * @param int $mapId
     * @param mixed $load
     * @return int
     */
    private function broadcastMapData(string $task, int $mapId, $load): int
    {
        $characterIds = $this->getCharacterIdsByMapId($mapId);
        $connections = $this->getConnectionsByCharacterIds($characterIds);

        $this->broadcast($connections, $this->newPayload($task, $load, $characterIds));

        return count($connections);
    }

    /**
     * set/update map access for allowed characterIds
     * @param string $task
     * @param array $accessData
     * @return int count of connected characters
     */
    private function setAccess(string $task, $accessData): int
    {
        $newMapCharacterIds = [];

        if ($mapId = (int)$accessData['id']) {
            $mapName = (string)$accessData['name'];
            $characterIds = (array)$accessData['characterIds'];
            // check all charactersIds that have map access... --------------------------------------------------------
            foreach ($characterIds as $characterId) {
                // ... for at least ONE active connection ...
                // ... and characterData cache exists for characterId
                if (
                    !empty($this->characters[$characterId]) &&
                    !empty($this->getCharacterData($characterId))
                ) {
                    $newMapCharacterIds[$characterId] = $characterId;
                }
            }

            $currentMapCharacterIds = (array)$this->subscriptions[$mapId]['characterIds'];

            // broadcast "map delete" to no longer valid characters ---------------------------------------------------
            $removedMapCharacterIds = array_keys(array_diff_key($currentMapCharacterIds, $newMapCharacterIds));
            $removedMapCharacterConnections = $this->getConnectionsByCharacterIds($removedMapCharacterIds);

            $this->broadcast($removedMapCharacterConnections, $this->newPayload($task, $mapId, $removedMapCharacterIds));

            // update map subscriptions -------------------------------------------------------------------------------
            if (!empty($newMapCharacterIds)) {
                // set new characters that have map access (overwrites existing subscriptions for that map)
                $this->subscriptions[$mapId]['characterIds'] = $newMapCharacterIds;
                $this->subscriptions[$mapId]['data']['name'] = $mapName;

                // check if subscriptions have changed
                if (!$this->arraysEqualKeys($currentMapCharacterIds, $newMapCharacterIds)) {
                    $this->broadcastMapSubscriptions([$mapId]);
                }
            } else {
                // no characters (left) on this map
                unset($this->subscriptions[$mapId]);
            }
        }
        return count($newMapCharacterIds);
    }

    /**
     * set map access data (whitelist) tokens for map access
     * @param $connectionAccessData
     * @return bool
     */
    private function setConnectionAccess($connectionAccessData)
    {
        $response = false;
        $characterId = (int)$connectionAccessData['id'];
        $characterData = $connectionAccessData['characterData'];
        $characterToken = $connectionAccessData['token'];

        if (
            $characterId &&
            $characterData &&
            $characterToken
        ) {
            // expire time for character and map tokens
            $expireTime = time() + $this->mapAccessExpireSeconds;

            // tokens for character access
            $this->characterAccessData[$characterId][] = [
                'token' => $characterToken,
                'expire' => $expireTime,
                'characterData' => $characterData
            ];

            foreach ((array)$connectionAccessData['mapData'] as $mapData) {
                $mapId = (int)$mapData['id'];

                $this->mapAccessData[$mapId][$characterId][] = [
                    'token' => $mapData['token'],
                    'expire' => $expireTime
                ];
            }

            $response = 'OK';
        }

        return $response;
    }

    /**
     * get stats data
     * -> lists all channels, subscribed characters + connection info
     * @return array
     */
    protected function getSubscriptionStats(): array
    {
        $uniqueConnections = [];
        $uniqueSubscriptions = [];
        $channelsStats = [];

        foreach ($this->subscriptions as $mapId => $subData) {
            $characterIds = $this->getCharacterIdsByMapId($mapId);
            $uniqueMapConnections = [];

            $channelStats = [
                'channelId'     => $mapId,
                'channelName'   => $subData['data']['name'],
                'countSub'      => count($characterIds),
                'countCon'      => 0,
                'subscriptions' => []
            ];

            foreach ($characterIds as $characterId) {
                $characterData = $this->getCharacterData($characterId);
                $connections = $this->getConnectionsByCharacterId($characterId);

                $characterStats = [
                    'characterId'   => $characterId,
                    'characterName' => isset($characterData['name']) ? $characterData['name'] : null,
                    'countCon'      => $connections->count(),
                    'connections'   => []
                ];

                foreach ($connections as $connection) {
                    if (!in_array($connection->resourceId, $uniqueMapConnections)) {
                        $uniqueMapConnections[] = $connection->resourceId;
                    }

                    $metaData = $this->getConnectionData($connection);
                    $microTime = (float)$metaData['mTimeSend'];
                    $logTime = Store::getDateTimeFromMicrotime($microTime);

                    $characterStats['connections'][] = [
                        'resourceId'        => $connection->resourceId,
                        'remoteAddress'     => $connection->remoteAddress,
                        'mTimeSend'         => $microTime,
                        'mTimeSendFormat1'  => $logTime->format('Y-m-d H:i:s.u'),
                        'mTimeSendFormat2'  => $logTime->format('H:i:s')
                    ];
                }

                $channelStats['subscriptions'][] = $characterStats;
            }

            $uniqueConnections = array_unique(array_merge($uniqueConnections, $uniqueMapConnections));
            $uniqueSubscriptions = array_unique(array_merge($uniqueSubscriptions, $characterIds));

            $channelStats['countCon'] = count($uniqueMapConnections);

            $channelsStats[] = $channelStats;
        }

        return [
            'countSub' => count($uniqueSubscriptions),
            'countCon' => count($uniqueConnections),
            'channels' => $channelsStats
        ];
    }

    /**
     * compare two assoc arrays by keys. Key order is ignored
     * -> if all keys from array1 exist in array2 && all keys from array2 exist in array 1, arrays are supposed to be equal
     * @param array $array1
     * @param array $array2
     * @return bool
     */
    protected function arraysEqualKeys(array $array1, array $array2): bool
    {
        return !array_diff_key($array1, $array2) && !array_diff_key($array2, $array1);
    }

    /**
     * dispatch log writing to a LogFileHandler
     * @param array $meta
     * @param array $log
     */
    private function handleLogData(array $meta, array $log)
    {
        $logHandler = new LogFileHandler((string)$meta['stream']);
        $logHandler->write($log);
    }

    /**
     * send JSON data to WebSocket connection
     * @param \Ratchet\ConnectionInterface $conn
     * @param array $obj
     */
    private function wsSendJson(ConnectionInterface $conn, array $obj): void
    {
        $json = json_encode($obj, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        error_log("[WS] send: " . $json);
        $conn->send($json);
    }

    /**
     * verify standalone ticket
     * @param string $ticket
     * @return array
     */
    private function standaloneTicketGet(string $ticket): array
    {
        $dir = '/var/www/html/pathfinder/tmp/standalone_ticket';
        $safe = preg_replace('/[^a-zA-Z0-9\-_]/', '', $ticket);
        if ($safe === '') return ['ok' => false, 'code' => 'bad_ticket'];


        $path = $dir . '/' . $safe;
        if (!is_file($path)) return ['ok' => false, 'code' => 'ticket_not_found'];


        $ttl = 60; // Standalone::TICKET_TTL_SECONDS와 맞춰라 (환경/상수로 가져와도 됨)
        $mt  = @filemtime($path);
        $now = time();

        if (!$mt) {
            @unlink($path);
            return ['ok' => false, 'code' => 'ticket_invalid_mtime'];
        }
        if (($now - $mt) > ($ttl + 2)) {
            @unlink($path);
            return ['ok' => false, 'code' => 'ticket_expired'];
        }

        $raw = @file_get_contents($path);
        if ($raw === false || trim($raw) === '') {
            @unlink($path);
            return ['ok' => false, 'code' => 'ticket_read_fail'];
        }

        $parts = explode('.', trim($raw));
        if (count($parts) < 2) {
            @unlink($path);
            return ['ok' => false, 'code' => 'ticket_corrupt'];
        }

        $cid = (int)$parts[0];
        $ts  = (int)$parts[1];

        if ($cid <= 0 || $ts <= 0) {
            @unlink($path);
            return ['ok' => false, 'code' => 'ticket_invalid'];
        }


        @unlink($path);

        return ['ok' => true, 'cid' => $cid, 'ttl' => $ttl, 'ts' => $ts];
    }

    /**
     * Standalone presence 전용 merge:
     * - id/name만 갱신
     * - corporation/alliance/log/rights/role/shared 등 기존 데이터는 절대 덮어쓰지 않음
     * => cron/백엔드가 ESI로 채운 위치(log)가 계속 유지되게 함
     */
    private function mergeCharacterPresenceOnly(array $presence): void
    {
        $cid = (int)($presence['id'] ?? 0);
        if ($cid <= 0) return;

        $name = trim((string)($presence['name'] ?? ''));
        if ($name === '') return;

        $existing = $this->getCharacterData($cid);
        if (!is_array($existing) || empty($existing)) {
            // 기존 데이터가 아예 없으면 최소 데이터로 시작
            $this->setCharacterData([
                'id'   => $cid,
                'name' => $name,
            ]);
            return;
        }

        // 기존 데이터 보존 + name만 업데이트
        $existing['id'] = $cid;
        $existing['name'] = $name;

        $this->setCharacterData($existing);
    }

    private function cleanupStandaloneExpiredPresence($conn, int $mapId): void
    {
        if (!isset($conn->standalonePresenceExp) || !is_array($conn->standalonePresenceExp)) return;

        $now = time();
        $expired = [];

        foreach ($conn->standalonePresenceExp as $cid => $exp) {
            $cid = (int)$cid;
            $exp = (int)$exp;
            if ($cid > 0 && $exp > 0 && $exp < $now) {
                $expired[] = $cid;
            }
        }

        if (empty($expired)) return;

        foreach ($expired as $cid) {
            // connection 레벨로 제거
            if (isset($this->characters[$cid][$conn->resourceId])) {
                unset($this->characters[$cid][$conn->resourceId]);
            }

            // 구독자에서도 제거
            if (isset($this->subscriptions[$mapId]['characterIds'][$cid])) {
                unset($this->subscriptions[$mapId]['characterIds'][$cid]);
            }

            unset($conn->standalonePresenceExp[$cid]);
        }

        // UI 갱신
        $this->broadcastMapSubscriptions([$mapId]);
    }
}

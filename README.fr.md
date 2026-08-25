# bpmn-provisioning-patterns

[![CI](https://github.com/jihedbfr-art/bpmn-provisioning-patterns/actions/workflows/ci.yml/badge.svg)](https://github.com/jihedbfr-art/bpmn-provisioning-patterns/actions)

[English version](./README.md)

La plupart des exemples publics Camunda sont des commandes de pizza : commande passée, paiement
prélevé, pizza livrée, fin. L'orchestration réelle n'a rien de cette propreté — des systèmes
externes qui ne répondent pas, des SLA qui forcent une décision quand même, et des rollbacks qui
doivent défaire un travail déjà effectué. Ce dépôt est un processus exécutable construit sur ces
contraintes-là : une saga de portabilité de numéro entre plusieurs opérateurs, modélisée sur la
façon dont ça fonctionne réellement entre opérateurs télécom, pas un diagramme inventé pour une
slide.

## Démarrage rapide

```bash
cp .env.example .env
docker compose up -d
```

Rendez-vous ensuite sur Camunda Cockpit : `http://localhost:8080/camunda` (identifiants : `demo` /
`demo`, ce sont des valeurs par défaut de dev, à surcharger avec `CAMUNDA_ADMIN_PASSWORD`).

Pour démarrer une saga :

```bash
curl -X POST http://localhost:8080/api/portability \
  -H "Content-Type: application/json" \
  -d '{"msisdn":"+21620000000","donorOperator":"Ooredoo","recipientOperator":"Orange"}'
```

Pour soumettre la réponse du donneur :

```bash
curl -X POST http://localhost:8080/api/portability/{requestId}/donor-response \
  -H "Content-Type: application/json" \
  -d '{"decision":"ACCEPTED"}'
```

## Le processus

Un abonné demande à transférer son numéro d'un opérateur donneur vers un opérateur receveur.
`number-portability-saga.bpmn` :

1. **Valider la demande** — msisdn et les deux opérateurs présents, donneur et receveur
   différents. Échoue bruyamment (pas silencieusement) sur une entrée invalide.
2. **Notifier l'opérateur donneur** — publie un événement Kafka. Dans un vrai déploiement, c'est
   ici qu'un message franchirait une frontière d'intégration inter-opérateurs (passerelle
   SOAP/REST, chambre de compensation MNP, ce qu'utilise le marché) ; ici, un topic Kafka tient
   lieu de cette frontière.
3. **Attendre la réponse du donneur — avec un SLA strict.** Modélisé comme un sous-processus
   embarqué (un événement intermédiaire de réception de message) avec un timer en frontière
   posé dessus. Si le donneur répond à temps, le message l'emporte. Sinon, le timer se déclenche
   et la saga suit le même chemin qu'un rejet explicite — un vrai SLA réglementaire ne se
   préoccupe pas de savoir *pourquoi* le donneur n'a pas répondu, seulement qu'il ne l'a pas fait.
4. **Accepté** → activation sur le réseau receveur, notification de fin, terminé.
   **Rejeté ou expiré** → compensation (annuler tout ce qui a déjà été provisionné) → une tâche
   humaine de **Révision manuelle**, parce qu'un vrai rejet a généralement besoin qu'une personne
   l'examine avant de clore le dossier, pas juste d'une nouvelle tentative automatique.

<details>
<summary>Pas issu du monde télécom ? Lisez cette correspondance</summary>

La plupart des exemples BPMN sont des commandes de pizza. Ce dépôt résout de vrais problèmes de
systèmes distribués, mais si le jargon télécom vous est étranger, voici l'équivalent exact côté
e-commerce :

- `Valider la demande` → `Valider le panier`
- `Notifier l'opérateur donneur` → `Demander l'autorisation de paiement`
- `Attendre la réponse du donneur (SLA)` → `Attendre la confirmation de paiement`
- `Activer chez le receveur` → `Réserver le stock`
- `Compenser / rollback` → `Rembourser le paiement`
- `Révision manuelle` → `File de révision anti-fraude`
- `Lot de SIM en masse` → `Traitement de commande en masse`
</details>

## Pourquoi un sous-processus embarqué pour le timeout, plutôt que deux chemins séparés

Un timer en frontière (boundary timer event) doit s'attacher à une activité, pas à un simple
événement de réception de message, donc le pattern « attendre un message avec un délai » exige que
l'événement de réception soit enveloppé dans un sous-processus, avec le timer posé sur la frontière
de ce sous-processus. Ça fait quelques éléments BPMN de plus que la version naïve, mais ça garantit
que le timeout et le rejet convergent vers la même tâche de compensation, au lieu d'avoir deux
copies de la même logique de rollback qui divergent avec le temps.

## Une deuxième saga : provisioning de SIM en masse

`bulk-sim-provisioning.bpmn` est une forme différente de la même idée sous-jacente — un échec
partiel dans un lot, et une règle qui décide si c'est acceptable ou s'il faut tout défaire.
Provisionnez un lot de SIM ; si le taux d'échec reste sous un seuil, le lot est accepté tel quel
(les échecs sont signalés pour une nouvelle tentative, les succès restent acquis) — s'il dépasse le
seuil, tout le lot est annulé en déprovisionnant exactement les SIM qui avaient réussi, pas celles
qui n'avaient jamais été provisionnées.

Celui-ci boucle sur le lot à l'intérieur d'une seule tâche de service, plutôt que de modéliser
chaque SIM comme une activité BPMN multi-instance. Le multi-instance rendrait chaque SIM
individuellement visible et reprenable dans Cockpit, ce qui est un vrai avantage pour certains cas
d'usage — mais agréger les résultats d'instances parallèles en une seule décision de taux d'échec
revient à lutter contre le scoping de variables par instance de Camunda, pour un bénéfice dont ce
cas précis n'a pas besoin. Personne ne met le lot en pause pour inspecter une SIM ; ce qui compte
ici, c'est le succès ou l'échec au niveau du lot.

`SimProvisioningGateway` est un placeholder pour ce que serait la vraie cible — une API EIR/HSS, un
appel back-office CRM. L'implémentation par défaut réussit toujours ; les tests contrôlent l'échec
par ICCID via un mock, délibérément pas via de l'aléatoire (un test qui échoue 1 fois sur 20 est
pire que pas de test du tout).

## Réconciliation

La conception événementielle suppose que chaque message finit par arriver. En pratique, certains
n'arrivent jamais — une intégration qui laisse tomber silencieusement un callback, un relecteur qui
oublie qu'une tâche de révision manuelle existe — et le timer de SLA seul ne le détecte pas, parce
qu'une saga peut être bloquée une étape *avant* même que le timer ne tourne, ou dans la tâche de
révision manuelle après que le timer a déjà fait son travail. `POST /api/reconciliation/run`
balaie chaque saga active, signale toute instance restée dans la même activité plus longtemps que
`provisioning.reconciliation.stuck-threshold` (15 minutes par défaut — délibérément plus court que
le SLA lui-même, pour que l'exploitation le sache avant le client), et publie un événement
`reconciliation.stuck_saga_detected` par instance bloquée. En production, ceci est câblé sur une
planification (cron, `@Scheduled` de Spring, ce que le déploiement utilise déjà pour ses jobs
batch) ; c'est exposé ici comme un endpoint surtout pour rester testable et déclenchable à la
demande.

## Outbox transactionnel & consommateur idempotent

Publier un événement vers Kafka depuis une transaction Camunda est un cas classique de **problème
de double écriture** : si la publication Kafka réussit mais que le commit en base échoue, un
événement fantôme est émis. Si la base commit mais que Kafka échoue, l'événement est perdu pour de
bon.
Pour corriger ça, nous avons implémenté le pattern **Outbox transactionnel** :
- Au lieu d'appeler `KafkaTemplate` directement, `OutboxPortabilityEventPublisher` insère un
  événement dans une table `portability_outbox` en utilisant **la même transaction base de
  données** que l'état du processus Camunda. Les deux réussissent ou échouent de façon atomique.
- Un relais en arrière-plan (`OutboxRelay`) interroge l'outbox à intervalles réguliers
  (`provisioning.outbox.relay.interval`, `PT1S` par défaut) via `SELECT ... FOR UPDATE SKIP LOCKED`
  pour verrouiller un lot sans bloquer les autres instances du relais. À noter : le relais est
  actuellement mono-instance, et le comportement `SKIP LOCKED` n'est pas couvert par un test de
  concurrence.
- Le relais publie de façon synchrone vers Kafka (`provisioning.outbox.relay.send-timeout`) et
  marque la ligne comme publiée.

Côté réception, consommer des événements exige de l'**idempotence**. Kafka garantit une livraison
au moins une fois, ce qui signifie qu'une réponse de donneur pourrait être traitée deux fois lors
d'une partition réseau ou d'un redémarrage de consommateur.
- Nous nous appuyons sur une table `processed_events` avec une contrainte unique sur `event_id`.
- Avant de corréler le message au moteur de processus, `ProcessedEventRepository` tente d'insérer
  l'`eventId`. Une `DuplicateKeyException` signifie qu'il a déjà été traité, ce qui permet de
  l'ignorer en toute sécurité.
- Si la corrélation échoue à cause d'un plantage applicatif ou base de données, la transaction fait
  un rollback, annulant à la fois l'insertion dans `processed_events` et l'état Camunda, ce qui
  garantit un rejeu sûr. En cas d'erreur de corrélation irrécupérable (par exemple, saga déjà
  terminée), le message part vers un Dead Letter Topic (DLT) après 3 tentatives.

## 🔥 Casse-le

Ce dépôt est construit pour être testé contre des pannes.

| # | Manipulation | Comportement réel attendu |
|---|---|---|
| 1 | `docker compose stop kafka` puis démarrer une saga | La saga **continue**, la notification est stockée en sécurité dans l'Outbox. Quand le broker redémarre, le relais la publie automatiquement. Aucun message perdu. |
| 2 | Démarrer une saga puis `docker compose restart app` | L'instance et son timer de SLA survivent au redémarrage (grâce à PostgreSQL). Essayez avec H2 pour voir le contraste. |
| 3 | Démarrer une saga et n'appeler jamais `donor-response` | Après `provisioning.sla.donor-response-timeout`, elle bascule automatiquement vers compensation + révision manuelle, exactement comme un rejet explicite. |
| 4 | Démarrer un lot de SIM avec un taux d'échec > `rollbackThreshold` | Tout le lot déclenche un rollback, mais seuls les ICCID *ayant réellement réussi* sont déprovisionnés. |
| 5 | Appeler `donor-response` plusieurs fois pour le même `requestId` | Le consommateur idempotent garantit que le processus n'avance qu'une seule fois. Les messages suivants sont ignorés grâce à `processed_events`. |

## Stack

Spring Boot 3.2, Camunda 7.23 (moteur embarqué — correspond à la façon dont ceci est réellement
déployé en pratique : le moteur de processus tourne à l'intérieur de l'application, pas comme un
cluster séparé), Kafka.

Une note sur Camunda 7 : l'édition communautaire ne reçoit plus de nouvelles versions —
l'investissement actuel de Camunda porte sur Camunda 8 (Zeebe), une architecture différente
(broker externe, pas embarqué). J'ai quand même utilisé la version 7 ici car le pattern présenté
dans ce dépôt — sagas, compensation, timeout-comme-rejet, révision humaine — est ce qui compte, pas
le moteur spécifique, et ça se transpose directement vers Camunda 8 ou tout autre orchestrateur. Le
portage vers la 8 est sur la roadmap.

## Le faire tourner

### Avec Docker (Postgres, durable)

Lancez `docker compose up -d`. Ceci démarre Kafka (KRaft), PostgreSQL, et l'application Spring
Boot avec le profil `postgres`. Ce mode est durable : les sagas et leurs timers survivront à un
redémarrage de l'application.

### Sans Docker (H2 en mémoire, le plus rapide)

Lancez simplement `mvn spring-boot:run`. L'application démarre instantanément avec une base H2 en
mémoire. Parfait pour un cycle de développement rapide, mais tout l'état est perdu au redémarrage.

Démarrez une demande de portabilité et soumettez une réponse de donneur avec les mêmes commandes
`curl` que dans la section **Démarrage rapide** ci-dessus.

Si personne n'appelle `donor-response` avant l'expiration du SLA défini dans
`provisioning.sla.donor-response-timeout`, la saga expire vers le même chemin de compensation +
révision manuelle qu'un rejet explicite.

Démarrez un lot de provisioning de SIM en masse :

```bash
curl -X POST localhost:8080/api/bulk-provisioning \
  -H "Content-Type: application/json" \
  -d '{"simRequests":[{"iccid":"8921...01","msisdn":"+21620000001"},{"iccid":"8921...02","msisdn":"+21620000002"}],"rollbackThreshold":0.2}'
# {"batchId":"...", "processInstanceId":"..."}
```

`rollbackThreshold` est optionnel (par défaut `provisioning.bulk-sim.default-rollback-threshold`,
0.2). Vérifiez le statut de la même façon : `curl localhost:8080/api/bulk-provisioning/{batchId}`.

## Tests

```bash
mvn test       # tests de processus contre le moteur H2 embarqué — pas de Docker
mvn verify     # lance aussi la vérification Testcontainers contre un vrai broker Kafka et une base PostgreSQL
```

`SagaDurabilityIT` prouve que l'instance de saga et son timer de frontière SLA survivent à un
redémarrage complet de l'application, en fermant puis rouvrant le contexte Spring contre la même
base PostgreSQL Testcontainers partagée. `NumberPortabilitySagaTest` fait avancer la saga à travers
le moteur embarqué de Camunda et vérifie l'état réel du processus — IDs d'activités actives, IDs
d'activités terminées dans l'historique, requêtes de tâches — pour les quatre chemins : acceptation
du donneur, rejet du donneur, expiration du SLA (via `ClockUtil` pour avancer l'horloge du moteur
et déclencher directement le job du timer de frontière, pas un vrai `Thread.sleep`), et une entrée
invalide qui n'atteint jamais l'étape de notification du donneur. `PortabilityEventPublisherIT`
démarre le contexte Spring complet contre un vrai broker Kafka via Testcontainers et relit un
événement publié pour confirmer que la config du producer et l'enveloppe JSON sont bien correctes.
`StuckSagaReconciliationServiceTest` couvre à la fois une saga fraîche (non signalée) et une
poussée au-delà du seuil de blocage, via la même manipulation d'horloge que le test de SLA.
`BulkSimProvisioningTest` couvre les cas succès total, taux d'échec au-dessus du seuil (rollback,
et seuls les ICCID ayant réussi sont déprovisionnés), et taux d'échec en dessous du seuil (succès
partiel accepté, pas de rollback) — avec la gateway mockée pour faire échouer des ICCID précis de
façon déterministe plutôt qu'aléatoire.

## Roadmap

- Porter vers Camunda 8 / Zeebe comme seconde implémentation, en parallèle, des mêmes patterns
- Câbler le balayage de réconciliation sur une vraie planification plutôt que sur un simple
  endpoint manuel
- Stratégies de purge pour `portability_outbox` et `processed_events`, pour éviter une croissance
  non bornée et une dégradation des index
- Mécanismes de retry et de rejeu pour les messages du Dead Letter Topic (DLT)
- Monitoring et alertes sur la métrique `provisioning.outbox.dead`
- Tests de concurrence multi-instance pour le relais outbox (validation de `SKIP LOCKED` à travers
  plusieurs nœuds)

## Licence

MIT — voir [LICENSE](LICENSE).

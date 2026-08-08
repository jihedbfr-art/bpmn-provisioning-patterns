<div align="center">
  <img src="./assets/brand/jihedailabs-logo.svg" alt="JihedAiLabs" width="180"/>
</div>

# BPMN Provisioning Patterns

<div align="center">

**Un projet <a href="https://github.com/jihedbfr-art">JihedAiLabs</a>** — Patterns d'orchestration de processus distribués et sagas d'activation avec Camunda, Spring Boot et Kafka.

<a href="./README.md">English version</a>

</div>

---

## Vue d'ensemble

`bpmn-provisioning-patterns` est un catalogue de référence implémentant les patterns d'orchestration résilients pour les architectures de systèmes distribués et le provisioning télécom/e-commerce.

- **Double narration :** Façade universelle e-commerce (commande, stock, paiement) et profondeur télécom réelle (activation SIM, MNP, provisioning 5G Core).
- **Résilience & Sagas :** Gestion automatisée des transactions compensatoires, gestion des pannes à mi-parcours et rééquilibrage Kafka.

## Reproductibilité

```bash
docker compose up -d
```

## Structure & Ingrédients
- Java 17 / Spring Boot 3
- Camunda BPMN 7.x / 8.x
- Apache Kafka & Testcontainers

# 1. Using a queue to generate reports

Date: 2024-06-05

## Status

2024-06-05 Proposed

## Context

Aamon may receive huge spikes of requests on the `/aamon/reports/:id_report/sidekiq-schedulation/:platform` path - in a short interval of time - to generate scheduled reports, at scheduled hours.

The path is associated to some work that is performed on the fly, which has some cascading effect both on the memory consumption and on the number of incoming APIs calls because of callbacks performed back by Hydra, eventually leading to crashes.  

## Decision

We are introducing a queue where requests to generate the scheduled reports are going to be pushed. This will decouple the incoming calls from the actual building of reports: Aamon instances can consume the queue at their own peace, leading to a more gradual memory consumption.

Since we don't need messages in the queue to follow any particular ordering, we're using a standard SQS Queue.\
Also, MessageVisibilityTimeout is set to 5 minutes because the actual incoming request to generate a report resolves to just performing an HTTP call to Hydra and then it just ends; the callback performed by Hydra to Aamon is what effectively starts the report generation. Hence, 5 minutes should be good enough for messages in the queue to be properly handled and deleted.

The queue comes with a DLQ associated to it: messages that are extracted more than 5 times from the queue are sent to the DLQ.

Both the queues have a retention period set to 14 days.

## Consequences

This opens the road to an architectural change that will gradually make the report generation event-driven, stateless and more resilient.
However, given the impact at infrastructure level, this change will have an impact on both DataLake V3 and DataLake V2 customers, because the endpoint to generate reports is the same in both the cases.

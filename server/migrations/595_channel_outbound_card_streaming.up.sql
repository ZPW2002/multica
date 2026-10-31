-- Streaming replies via the Feishu cardkit API.
--
-- card_entity_id is the cardkit card entity (POST /cardkit/v1/cards) that
-- content/settings updates address — distinct from channel_card_message_id,
-- which is only the delivered message carrying the card. last_sequence
-- persists the last accepted cardkit operation sequence: every cardkit call
-- must carry a strictly greater sequence, so a recovery path on another
-- process reads it from here instead of the in-memory counter.
ALTER TABLE channel_outbound_card_message
    ADD COLUMN IF NOT EXISTS card_entity_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS last_sequence INT NOT NULL DEFAULT 0;

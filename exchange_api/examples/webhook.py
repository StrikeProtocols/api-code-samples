from exchange_api.client import Client


def test_webhooks(client: Client):
    client.list_webhooks()
    client.set_webhook_config('https://url1.com', 3, 1, 'test1@test.com')
    client.set_webhook_config('https://url2.com', 3, 1, 'test2@test.com')

    webhooks_response = client.list_webhooks(from_sequence_number=0, undelivered=True)
    webhooks = webhooks_response['webhooks']
    client.mark_webhooks_as_delivered([w['sequenceNumber'] for w in webhooks])
    client.get_webhook(webhooks[0]['sequenceNumber'])

    # this should be empty now since all the webhooks were marked as delivered
    assert client.list_webhooks(from_sequence_number=0, undelivered=True)['webhooks'] == []

    assert client.get_webhook_config()['url'] == 'https://url2.com'
    client.delete_webhook_config()
    client.get_webhook_config(expected_status_code=404)

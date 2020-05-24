import json

import requests

from airflow import AirflowException
from airflow.hooks.http_hook import HttpHook

class WechatHook(HttpHook):

    def __init__(self,
                 wechat_conn_id='wechat_default',
                 message_type='text',
                 message=None,
                 at_mobiles=None,
                 at_all=False,
                 *args,
                 **kwargs
                 ):
        super(WechatHook, self).__init__(http_conn_id=wechat_conn_id, *args, **kwargs)
        self.message_type = message_type
        self.message = message
        self.at_mobiles = at_mobiles
        self.at_all = at_all

    def _get_endpoint(self):
        """
        Get WeChat endpoint for sending message.
        """
        conn = self.get_connection(self.http_conn_id)
        token = conn.password
        if not token:
            raise AirflowException('WeChat robot key is requests but get nothing, '
                                   'put your robot key in password input box of connection configuration.')
        return 'cgi-bin/webhook/send?key={}'.format(token)

    def _build_message(self):
        """
        Build different type of WeChat message
        As most commonly used type, text message just need post message content
        rather than a dict like ``{'content': 'message'}``
        """
        if self.message_type in ['text', 'markdown']:
            data = {
                'msgtype': self.message_type,
                self.message_type: {
                    'content': self.message
                } if self.message_type == 'text' else self.message,
                'at': {
                    'atMobiles': self.at_mobiles,
                    'isAtAll': self.at_all
                }
            }
        else:
            data = {
                'msgtype': self.message_type,
                self.message_type: self.message
            }
        return json.dumps(data)

    def get_conn(self, headers=None):
        """
        Overwrite HttpHook get_conn because just need base_url and headers and
        not don't need generic params

        :param headers: additional headers to be passed through as a dictionary
        :type headers: dict
        """
        conn = self.get_connection(self.http_conn_id)
        self.base_url = conn.host if conn.host else 'https://qyapi.weixin.qq.com'
        session = requests.Session()
        if headers:
            session.headers.update(headers)
        return session

    def send(self):
        """
        Send WeChat message
        """
        support_type = ['text', 'link', 'markdown', 'actionCard', 'feedCard']
        if self.message_type not in support_type:
            raise ValueError('WeChatWebhookHook only support {} '
                             'so far, but receive {}'.format(support_type, self.message_type))

        data = self._build_message()
        self.log.info('Sending WeChat type %s message %s', self.message_type, data)
        resp = self.run(endpoint=self._get_endpoint(),
                        data=data,
                        headers={'Content-Type': 'application/json'})

        # WeChat success send message will with errcode equal to 0
        if int(resp.json().get('errcode')) != 0:
            raise AirflowException('Send WeChat message failed, receive error '
                                   'message %s', resp.text)
        self.log.info('Success Send WeChat message')
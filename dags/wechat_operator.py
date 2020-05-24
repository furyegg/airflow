from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults

from wechat_hook import WechatHook

class WechatOperator(BaseOperator):

    template_fields = ('message',)

    ui_color = '#4ea4d4'  # Wechat icon color

    @apply_defaults
    def __init__(self,
                 wechat_conn_id='wechat_default',
                 message_type='text',
                 message=None,
                 at_mobiles=None,
                 at_all=False,
                 *args,
                 **kwargs):
        super(WechatOperator, self).__init__(*args, **kwargs)
        self.wechat_conn_id = wechat_conn_id
        self.message_type = message_type
        self.message = message
        self.at_mobiles = at_mobiles
        self.at_all = at_all

    def execute(self, context):
        self.log.info('Sending WeChat message.')
        hook = WechatHook(
            self.wechat_conn_id,
            self.message_type,
            self.message,
            self.at_mobiles,
            self.at_all
        )
        hook.send()
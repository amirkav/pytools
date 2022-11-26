import json
from typing import Dict, Optional

import requests

from tools.logger import Logger
from tools.retry_backoff_class import RetryAndCatch

NOTIFICATION_TYPE_TO_ICON = {
    "error": ":red_circle:",
    "success": ":white_check_mark:",
    "info": ":information_source:",
}


class SlackInternalError(Exception):
    status_code = 500


class SlackBadRequest(SlackInternalError):
    status_code = 400


class SlackNotifications:
    def __init__(
        self,
        slack_webhook_url: str,
        slack_channel: str = "notifications",
        slack_username: Optional[str] = None,
        slack_icon: Optional[str] = None,
    ) -> None:
        self.slack_notification_target = "slack"
        self.slack_webhook_url = slack_webhook_url
        self.slack_icon = slack_icon
        self.slack_channel = slack_channel
        self.slack_username = slack_username
        self._logger = Logger(__name__)

    @RetryAndCatch(
        num_tries=2,
        exceptions_to_catch=(SlackInternalError,),
    )
    def send_notification(self, message: str, notification_type: str = "success") -> None:
        """
        Sends a message to the configured slack channel.

        Arguments:
            message {str} -- message to send to slack channel
            notification_type {str} -- notification type of message. This will only
                effect the message icon.
        """
        self._logger.info(message)
        slack_message = self._generate_slack_message(message, notification_type)
        slack_data = self._generate_slack_notification_data(slack_message)
        res = requests.post(self.slack_webhook_url, data=json.dumps(slack_data))
        self._logger.info(f"Slack response status code: {res.status_code}")
        if res.status_code == 500:
            raise SlackInternalError
        if res.status_code == 400:
            raise SlackBadRequest

    def _generate_slack_notification_data(self, message: str) -> Dict[str, str]:
        """
        Generates the data to send to the slack API

        Arguments:
            message -- message to send to slack channel

        Returns:
            Dict of slack data to send to the slack API
        """
        slack_data = dict()
        slack_data["text"] = message

        # Parse the override params
        if self.slack_icon:
            slack_data["icon_url"] = self.slack_icon
        if self.slack_channel:
            slack_data["channel"] = self.slack_channel
        if self.slack_username:
            slack_data["username"] = self.slack_username

        return slack_data

    def _generate_slack_message(self, message: str, notification_type: str) -> str:
        """Generates the slack message.
        This concats the notification type icon and message
        into a single string.

        Arguments:
            message {str} -- message to send to slack channel
            notification_type {str} -- notification type of message.
                This will only effect the message icon.

        Returns:
            {str} -- formatted slack message
        """
        return "{icon} {message}".format(
            icon=self._get_notification_type_icon(notification_type), message=message
        )

    @staticmethod
    def _get_notification_type_icon(notification_type: str) -> str:
        """Gets the icon for a given notification type.
        If not found it will return an empty string.

        Arguments:
            notification_type {str} -- notification type of message.

        Returns:
            str -- icon code to be used for slack. O.w. empty string.
        """
        return NOTIFICATION_TYPE_TO_ICON.get(notification_type, "")

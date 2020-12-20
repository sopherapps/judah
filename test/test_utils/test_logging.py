"""Module containing tests for the logging utility functions"""

import logging
import os
from sys import getsizeof
from unittest import TestCase, main
from judah.utils.logging import setup_rotating_file_logger


_PARENT_FOLDER = os.path.dirname(__file__)
_MOCK_ASSET_FOLDER_PATH = os.path.join(os.path.dirname(_PARENT_FOLDER), 'assets')
_TEXT_OF_5000_BYTES = """\
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Proin maximus, justo placerat tristique blandit, metus quam semper est, ac posuere metus lacus id ante. Orci varius natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Morbi nec eros in lorem accumsan hendrerit. In varius risus in lectus interdum ornare. Quisque ut massa elit. Vestibulum sit amet neque lorem. Integer porttitor est nec euismod placerat.

Ut venenatis blandit tincidunt. Vestibulum bibendum orci purus, auctor posuere quam gravida ac. Vestibulum vehicula, ante ac molestie sagittis, sem sapien varius ante, vitae lacinia tortor tortor eget dui. Pellentesque commodo tempor tempus. Curabitur fringilla maximus massa id eleifend. Ut maximus felis ut faucibus lacinia. Phasellus hendrerit velit lorem, in varius nisl pretium et. Sed mattis turpis dolor, non finibus orci porta ut. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Maecenas ullamcorper malesuada facilisis. In sem turpis, aliquam ac orci in, vehicula ullamcorper sem. Etiam nec blandit sem. Praesent sit amet pharetra felis.

Aliquam erat volutpat. Donec non magna a dolor convallis accumsan. Nulla efficitur a lacus in sollicitudin. Nullam ac pellentesque metus, sed aliquam sapien. Suspendisse eu ex vel augue vulputate pharetra. Quisque eu rhoncus augue. Nullam pulvinar nulla lacus, eu finibus magna mattis vel. Donec convallis a sapien non maximus. Donec sollicitudin tempus leo, vel varius libero rhoncus non. Duis porttitor orci et felis bibendum, at pharetra elit vulputate. In hac habitasse platea dictumst. Sed dolor odio, rhoncus nec eleifend eget, consectetur vehicula ante. Vestibulum dignissim sit amet felis eu pellentesque.

Vivamus purus tortor, lobortis vitae sodales quis, auctor non diam. Donec rutrum risus vitae nisi tristique auctor. Donec tempor sed eros sed ultrices. Maecenas id congue ipsum. Etiam blandit tincidunt magna, a commodo mi tristique sit amet. Proin vulputate tempus aliquet. Donec ultricies ipsum et sodales molestie. Aenean dignissim diam nisl, a eleifend nulla pharetra accumsan. Curabitur scelerisque quam sed dolor iaculis, vitae dapibus turpis accumsan. Ut quis leo lectus. Nunc at suscipit ante. Duis non consequat turpis. Mauris nisi nibh, pellentesque id neque sed, ultrices congue urna. Proin blandit nec risus eu lobortis.

Vestibulum aliquet lectus elit, id vestibulum felis convallis at. Suspendisse semper elit sed leo ultrices, et lacinia orci vestibulum. Nam ullamcorper pharetra nulla, gravida efficitur urna rhoncus in. Praesent ultricies urna vel pretium interdum. Vivamus luctus ultrices dignissim. Ut vel mollis lectus. In rhoncus tortor sed nisi tincidunt, ac luctus dolor gravida.

Cras convallis sapien nisl, a ultrices ex porta nec. In vitae lectus ut nisi consectetur tempus. Duis vel lacus eget tellus feugiat vestibulum in vel tellus. Nulla eget turpis neque. Aliquam finibus rhoncus pretium. Aliquam ac mi eu augue sagittis pellentesque at ac quam. Proin at augue id neque imperdiet rutrum. Ut egestas eleifend massa ut egestas. Vestibulum malesuada eros eget gravida fermentum. Ut in luctus ante. Curabitur sodales feugiat risus. Etiam pulvinar bibendum ornare. Suspendisse gravida vel nisi vitae volutpat. Mauris aliquam, risus quis placerat accumsan, eros neque euismod eros, id vestibulum purus nisl a velit.

Donec non enim est. Nulla diam metus, ultricies eget dui vel, feugiat ultricies magna. Nunc efficitur leo sed pulvinar mattis. Vestibulum dictum odio sed sapien fermentum finibus. Praesent aliquam hendrerit quam vitae eleifend. Ut bibendum fringilla ligula, non rutrum purus. Donec in libero cursus, elementum est eu, finibus dolor. Cras semper iaculis dignissim. Donec elementum mi ante, at tempus nisi accumsan in. In placerat, tellus ut imperdiet sagittis, erat mauris imperdiet nunc, quis convallis nisi lectus sit amet odio.

Sed tincidunt ut erat eu tempus. Nullam id est sit amet velit gravida mollis. Sed iaculis mi nec velit efficitur molestie. Ut sed ligula libero. Fusce neque erat, malesuada vel rutrum at, mattis id augue. Integer vel diam a enim congue ultrices ac at mauris. Morbi vestibulum porttitor massa eu sagittis. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas.

Mauris commodo accumsan convallis. Donec rutrum aliquam nulla, eget pretium sem varius quis. Aenean nec sem vestibulum, tristique tortor et, semper arcu. Nulla facilisi. In hac habitasse platea dictumst. Etiam a ligula eu felis semper malesuada non in nulla. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Curabitur faucibus tincidunt mattis. Etiam id nunc leo. Aenean imperdiet risus sit amet nunc semper posuere. Etiam in nibh id felis accumsan porta nec quis nibh. Nam mollis tortor vitae turpis pellentesque rutrum. Mauris porta turpis et orci volutpat auctor. Quisque quis interdum velit. Pellentesque ut nisl non nf..
"""


class TestLoggingUtilities(TestCase):
    """Tests for the logging utility functions"""

    def setUp(self) -> None:
        """Initialize some common variables"""
        self.log_file_path = os.path.join(_MOCK_ASSET_FOLDER_PATH, 'error.log')
        self.log_backup_file_path = os.path.join(_MOCK_ASSET_FOLDER_PATH, 'error.log.1')

    def test_setup_rotating_file_logger(self):
        """Makes the logger to use a file that rotates when a given size is reached"""
        logger = logging.getLogger('random-test')

        max_bytes = getsizeof(_TEXT_OF_5000_BYTES)
        self.assertEqual(max_bytes, 5000)

        setup_rotating_file_logger(logger=logger, file_path=self.log_file_path, max_bytes=max_bytes)

        self.assertFalse(os.path.isfile(self.log_backup_file_path))

        logger.error(_TEXT_OF_5000_BYTES)
        logger.error(_TEXT_OF_5000_BYTES)
        self.assertTrue(os.path.isfile(self.log_backup_file_path))

        file_size_of_log_backup = os.path.getsize(self.log_backup_file_path)
        file_size_of_log = os.path.getsize(self.log_file_path)
        max_possible_size = 5000 + 1

        self.assertLessEqual(file_size_of_log, max_possible_size)
        self.assertLessEqual(file_size_of_log_backup, max_possible_size)

    def tearDown(self) -> None:
        """Clean up"""
        if os.path.isfile(self.log_file_path):
            os.remove(self.log_file_path)

        if os.path.isfile(self.log_backup_file_path):
            os.remove(self.log_backup_file_path)


if __name__ == '__main__':
    main()

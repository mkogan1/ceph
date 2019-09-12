import { Helper } from '../helper.po';
import { FilesystemsPageHelper } from './filesystems.po';

describe('Filesystems page', () => {
  let filesystems: FilesystemsPageHelper;

  beforeAll(() => {
    filesystems = new FilesystemsPageHelper();
  });

  afterEach(async () => {
    await Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(async () => {
      await filesystems.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      await filesystems.waitTextToBePresent(filesystems.getBreadcrumb(), 'Filesystems');
    });
  });
});

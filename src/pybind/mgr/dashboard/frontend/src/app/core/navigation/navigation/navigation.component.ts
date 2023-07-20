import { Component, HostBinding, OnDestroy, OnInit } from '@angular/core';

import * as _ from 'lodash';
import { Subscription } from 'rxjs';

import { Icons } from '~/app/shared/enum/icons.enum';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { CallHomeNotificationService } from '~/app/shared/services/call-home-notification.service';
import {
  FeatureTogglesMap$,
  FeatureTogglesService
} from '~/app/shared/services/feature-toggles.service';
import { MotdNotificationService } from '~/app/shared/services/motd-notification.service';
import { PrometheusAlertService } from '~/app/shared/services/prometheus-alert.service';
import { SummaryService } from '~/app/shared/services/summary.service';
import { TelemetryNotificationService } from '~/app/shared/services/telemetry-notification.service';
import { environment } from '~/environments/environment';
import { StorageInsightsNotificationService } from '../../../shared/services/storage-insights-notification.service';

@Component({
  selector: 'cd-navigation',
  templateUrl: './navigation.component.html',
  styleUrls: ['./navigation.component.scss']
})
export class NavigationComponent implements OnInit, OnDestroy {
  notifications: string[] = [];
  @HostBinding('class') get class(): string {
    return 'top-notification-' + this.notifications.length;
  }

  permissions: Permissions;
  enabledFeature$: FeatureTogglesMap$;
  summaryData: any;
  icons = Icons;

  rightSidebarOpen = false; // rightSidebar only opens when width is less than 768px
  showMenuSidebar = true;
  displayedSubMenu = '';

  simplebar = {
    autoHide: false
  };
  private subs = new Subscription();

  evironment = environment;

  constructor(
    private authStorageService: AuthStorageService,
    private summaryService: SummaryService,
    private featureToggles: FeatureTogglesService,
    private telemetryNotificationService: TelemetryNotificationService,
    public prometheusAlertService: PrometheusAlertService,
    private motdNotificationService: MotdNotificationService,
    private callHomeNotificationService: CallHomeNotificationService,
    private storageInsightsNotificationService: StorageInsightsNotificationService
  ) {
    this.permissions = this.authStorageService.getPermissions();
    this.enabledFeature$ = this.featureToggles.get();
  }

  ngOnInit() {
    this.subs.add(
      this.summaryService.subscribe((summary) => {
        this.summaryData = summary;
      })
    );
    /*
     Note: If you're going to add more top notifications please do not forget to increase
     the number of generated css-classes in section topNotification settings in the scss
     file.
     */
    this.subs.add(
      this.authStorageService.isPwdDisplayed$.subscribe((isDisplayed) => {
        this.showTopNotification('isPwdDisplayed', isDisplayed);
      })
    );
    this.subs.add(
      this.telemetryNotificationService.update.subscribe((visible: boolean) => {
        this.showTopNotification('telemetryNotificationEnabled', visible);
      })
    );
    this.subs.add(
      this.motdNotificationService.motd$.subscribe((motd: any) => {
        this.showTopNotification('motdNotificationEnabled', _.isPlainObject(motd));
      })
    );

    if (this.evironment.build === 'ibm') {
      this.subs.add(
        this.callHomeNotificationService.remindLaterOn$.subscribe((visible: boolean) => {
          this.showTopNotification('callHomeNotificationEnabled', visible);
        })
      );
      this.subs.add(
        this.storageInsightsNotificationService.remindLaterOn$.subscribe((visible: boolean) => {
          this.showTopNotification('storagteInsightsEnabled', visible);
        })
      );
    }
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }

  blockHealthColor() {
    if (this.summaryData && this.summaryData.rbd_mirroring) {
      if (this.summaryData.rbd_mirroring.errors > 0) {
        return { color: '#f4926c' };
      } else if (this.summaryData.rbd_mirroring.warnings > 0) {
        return { color: '#f0ad4e' };
      }
    }

    return undefined;
  }

  toggleSubMenu(menu: string) {
    if (this.displayedSubMenu === menu) {
      this.displayedSubMenu = '';
    } else {
      this.displayedSubMenu = menu;
    }
  }

  toggleRightSidebar() {
    this.rightSidebarOpen = !this.rightSidebarOpen;
  }

  showTopNotification(name: string, isDisplayed: boolean) {
    if (isDisplayed) {
      if (!this.notifications.includes(name)) {
        this.notifications.push(name);
      }
    } else {
      const index = this.notifications.indexOf(name);
      if (index >= 0) {
        this.notifications.splice(index, 1);
      }
    }
  }
}

import { Component, OnDestroy, OnInit } from '@angular/core';

import { detect } from 'detect-browser';
import { BsModalRef } from 'ngx-bootstrap/modal';
import { Subscription } from 'rxjs';
import { environment } from '../../../../environments/environment';
import { UserService } from '../../../shared/api/user.service';
import { AppConstants } from '../../../shared/constants/app.constants';
import { Permission } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { SummaryService } from '../../../shared/services/summary.service';

@Component({
  selector: 'cd-about',
  templateUrl: './about.component.html',
  styleUrls: ['./about.component.scss']
})
export class AboutComponent implements OnInit, OnDestroy {
  modalVariables: any;
  versionNumber: string;
  versionHash: string;
  versionName: string;
  subs: Subscription;
  userPermission: Permission;
  projectConstants: typeof AppConstants;
  hostAddr: string;
  copyright: string;

  constructor(
    public modalRef: BsModalRef,
    private summaryService: SummaryService,
    private userService: UserService,
    private authStorageService: AuthStorageService
  ) {
    this.userPermission = this.authStorageService.getPermissions().user;
  }

  ngOnInit() {
    this.projectConstants = AppConstants;
    this.copyright = 'Copyright © ' + environment.year + this.projectConstants.contributors;
    this.hostAddr = window.location.hostname;
    this.modalVariables = this.setVariables();
    this.subs = this.summaryService.subscribe((summary: any) => {
      if (!summary) {
        return;
      }
      this.hostAddr = summary.mgr_host.replace(/(^\w+:|^)\/\//, '').replace(/\/$/, '');
    });
    const element = document.getElementsByTagName('div');
    for (let i = 0; i < element.length; ++i) {
      if (element[i].className === 'modal-content') {
        element[i].classList.add('about_modal_branding');
      }
    }

    //    element[0].classList.add('abc');
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }

  setVariables() {
    const project = {} as any;
    project.user = localStorage.getItem('dashboard_username');
    project.role = 'user';
    if (this.userPermission.read) {
      this.userService.get(project.user).subscribe((data: any) => {
        project.role = data.roles;
      });
    }
    const browser = detect();
    project.browserName = browser && browser.name ? browser.name : 'Not detected';
    project.browserVersion = browser && browser.version ? browser.version : 'Not detected';
    project.browserOS = browser && browser.os ? browser.os : 'Not detected';
    return project;
  }
}

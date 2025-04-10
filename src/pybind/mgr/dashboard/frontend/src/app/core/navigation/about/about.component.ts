import { Component, OnDestroy, OnInit } from '@angular/core';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { detect } from 'detect-browser';
import { Subscription } from 'rxjs';

import { UserService } from '~/app/shared/api/user.service';
import { AppConstants } from '~/app/shared/constants/app.constants';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { SummaryService } from '~/app/shared/services/summary.service';

@Component({
  selector: 'cd-about',
  templateUrl: './about.component.html',
  styleUrls: ['./about.component.scss']
})
export class AboutComponent implements OnInit, OnDestroy {
  modalVariables: any;
  versionHash: string;
  versionName: string;
  subs: Subscription;
  userPermission: Permission;
  projectConstants: typeof AppConstants;
  hostAddr: string;
  copyright: string;
  version: string;
  upstreamVersion: string;

  constructor(
    public activeModal: NgbActiveModal,
    private summaryService: SummaryService,
    private userService: UserService,
    private authStorageService: AuthStorageService
  ) {
    this.userPermission = this.authStorageService.getPermissions().user;
  }

  ngOnInit() {
    this.projectConstants = AppConstants;
    this.hostAddr = window.location.hostname;
    this.modalVariables = this.setVariables();
    this.subs = this.summaryService.subscribe((summary) => {
      this.upstreamVersion = summary.upstream_version.replace('ceph version ', '').split(' ')[0];
      this.version = summary.version;
      this.hostAddr = summary.mgr_host.replace(/(^\w+:|^)\/\//, '').replace(/\/$/, '');
    });
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

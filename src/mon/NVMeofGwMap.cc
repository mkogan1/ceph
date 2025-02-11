// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include <boost/tokenizer.hpp>
#include "include/stringify.h"
#include "NVMeofGwMon.h"
#include "NVMeofGwMap.h"
#include "OSDMonitor.h"

using std::map;
using std::make_pair;
using std::ostream;
using std::ostringstream;
using std::string;

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix *_dout << "nvmeofgw " << __PRETTY_FUNCTION__ << " "

void NVMeofGwMap::to_gmap(std::map<NvmeGroupKey, NvmeGwMap>& Gmap) const {
    Gmap.clear();
    for (const auto& created_map_pair: Created_gws) {
        const auto& group_key = created_map_pair.first;
        const NvmeGwCreatedMap& gw_created_map = created_map_pair.second;
        for (const auto& gw_created_pair: gw_created_map) {
            const auto& gw_id = gw_created_pair.first;
            const auto& gw_created  = gw_created_pair.second;

            auto gw_state = NvmeGwState(gw_created.ana_grp_id, epoch, gw_created.availability);
            for (const auto& sub: gw_created.subsystems) {
                gw_state.subsystems.insert({sub.nqn, NqnState(sub.nqn, gw_created.sm_state, gw_created )});
            }
            Gmap[group_key][gw_id] = gw_state;
        }
    }
}

int  NVMeofGwMap::cfg_add_gw(const NvmeGwId &gw_id, const NvmeGroupKey& group_key) {
    // Calculate allocated group bitmask
    bool allocated[MAX_SUPPORTED_ANA_GROUPS] = {false};
    for (auto& itr: Created_gws[group_key]) {
        allocated[itr.second.ana_grp_id] = true;
        if(itr.first == gw_id) {
            dout(1) << __func__ << " ERROR create GW: already exists in map " << gw_id << dendl;
            return -EEXIST ;
        }
    }

    // Allocate the new group id
    for(int i=0; i<=MAX_SUPPORTED_ANA_GROUPS; i++) {
        if (allocated[i] == false) {
            NvmeGwCreated gw_created(i);
            Created_gws[group_key][gw_id] = gw_created;
            Created_gws[group_key][gw_id].performed_full_startup = true;
            dout(4) << __func__ << "Created GWS:  " << Created_gws  <<  dendl;
            return 0;
        }
    }
    dout(1) << __func__ << " ERROR create GW: " << gw_id << "   ANA groupId was not allocated "   << dendl;
    return -EINVAL;
}

int NVMeofGwMap::cfg_delete_gw(const NvmeGwId &gw_id, const NvmeGroupKey& group_key) {
    int rc = 0;
    for (auto& gws_states: Created_gws[group_key]) {

        if (gws_states.first == gw_id) {
            auto& state = gws_states.second;
            for(int i=0; i<MAX_SUPPORTED_ANA_GROUPS; i++){
                bool modified;
                fsm_handle_gw_delete (gw_id, group_key, state.sm_state[i], i, modified);
            }
            dout(4) << " Delete GW :"<< gw_id  << " ANA grpid: " << state.ana_grp_id  << dendl;
            Gmetadata[group_key].erase(gw_id);
            if(Gmetadata[group_key].size() == 0)
                Gmetadata.erase(group_key);

            Created_gws[group_key].erase(gw_id);
            if(Created_gws[group_key].size() == 0)
                Created_gws.erase(group_key);
            return rc;
        }
    }

    return -EINVAL;
}


int NVMeofGwMap::process_gw_map_gw_down(const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
                                            bool &propose_pending) {
    int rc = 0;
    auto& gws_states = Created_gws[group_key];
    auto  gw_state = gws_states.find(gw_id);
    if (gw_state != gws_states.end()) {
        dout(4) << "GW down " << gw_id << dendl;
        auto& st = gw_state->second;
        st.set_unavailable_state();
        for (NvmeAnaGrpId i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i ++) {
            fsm_handle_gw_down (gw_id, group_key, st.sm_state[i], i, propose_pending);
            st.standby_state(i);
        }
        propose_pending = true; // map should reflect that gw becames unavailable
        if (propose_pending) validate_gw_map(group_key);
    }
    else {
        dout(1)  << __FUNCTION__ << "ERROR GW-id was not found in the map " << gw_id << dendl;
        rc = -EINVAL;
    }
    return rc;
}


void NVMeofGwMap::process_gw_map_ka(const NvmeGwId &gw_id, const NvmeGroupKey& group_key,  epoch_t& last_osd_epoch, bool &propose_pending)
{
    auto& gws_states = Created_gws[group_key];
    auto  gw_state = gws_states.find(gw_id);
    ceph_assert (gw_state != gws_states.end());
    auto& st = gw_state->second;
    dout(20)  << "KA beacon from the GW " << gw_id << " in state " << (int)st.availability << dendl;

    if (st.availability == GW_AVAILABILITY_E::GW_CREATED) {
        // first time appears - allow IO traffic for this GW
        st.availability = GW_AVAILABILITY_E::GW_AVAILABLE;
        for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++) st.sm_state[i] = GW_STATES_PER_AGROUP_E::GW_STANDBY_STATE;
        if (st.ana_grp_id != REDUNDANT_GW_ANA_GROUP_ID) { // not a redundand GW
            st.active_state(st.ana_grp_id);
        }
        propose_pending = true;
    }
    else if (st.availability == GW_AVAILABILITY_E::GW_UNAVAILABLE) {
        st.availability = GW_AVAILABILITY_E::GW_AVAILABLE;
        if (st.ana_grp_id == REDUNDANT_GW_ANA_GROUP_ID) {
            for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++) st.sm_state[i] = GW_STATES_PER_AGROUP_E::GW_STANDBY_STATE;
            propose_pending = true; //TODO  try to find the 1st GW overloaded by ANA groups and start  failback for ANA group that it is not an owner of
        }
        else {
            //========= prepare to Failback to this GW =========
            // find the GW that took over on the group st.ana_grp_id
            find_failback_gw(gw_id, group_key, propose_pending);
        }
    }
    else if (st.availability == GW_AVAILABILITY_E::GW_AVAILABLE) {
        for(int i=0; i<MAX_SUPPORTED_ANA_GROUPS; i++){
          fsm_handle_gw_alive (gw_id, group_key, gw_state->second, st.sm_state[i], i, last_osd_epoch, propose_pending);
        }
    }
    if (propose_pending) validate_gw_map(group_key);
}


void NVMeofGwMap::handle_abandoned_ana_groups(bool& propose)
{
    propose = false;
    for (auto& group_state: Created_gws) {
        auto& group_key = group_state.first;
        auto& gws_states = group_state.second;

            for (auto& gw_state : gws_states) { // loop for GWs inside nqn group
                auto& gw_id = gw_state.first;
                NvmeGwCreated& state = gw_state.second;

                //1. Failover missed : is there is a GW in unavailable state? if yes, is its ANA group handled by some other GW?
                if (state.availability == GW_AVAILABILITY_E::GW_UNAVAILABLE && state.ana_grp_id != REDUNDANT_GW_ANA_GROUP_ID) {
                    auto found_gw_for_ana_group = false;
                    for (auto& gw_state2 : gws_states) {
                        NvmeGwCreated& state2 = gw_state2.second;
                        if (state2.availability == GW_AVAILABILITY_E::GW_AVAILABLE && state2.sm_state[state.ana_grp_id] == GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE) {
                            found_gw_for_ana_group = true; // dout(4) << "Found GW " << ptr2.first << " that handles ANA grp " << (int)state->optimized_ana_group_id << dendl;
                            break;
                        }
                    }
                    if (found_gw_for_ana_group == false) { //choose the GW for handle ana group
                        dout(4)<< "Was not found the GW " << " that handles ANA grp " << (int)state.ana_grp_id << " find candidate "<< dendl;

                        for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++)
                            find_failover_candidate( gw_id, group_key, i, propose );
                    }
                }

                //2. Failback missed: Check this GW is Available and Standby and no other GW is doing Failback to it
                else if (state.availability == GW_AVAILABILITY_E::GW_AVAILABLE
                            && state.ana_grp_id != REDUNDANT_GW_ANA_GROUP_ID &&
                            state.sm_state[state.ana_grp_id] == GW_STATES_PER_AGROUP_E::GW_STANDBY_STATE)
                {
                    find_failback_gw(gw_id, group_key, propose);
                }
            }
            if (propose) {
                validate_gw_map(group_key);
            }
    }
}


void  NVMeofGwMap::set_failover_gw_for_ANA_group(const NvmeGwId &failed_gw_id, const NvmeGroupKey& group_key, const NvmeGwId &gw_id,  NvmeAnaGrpId ANA_groupid)
{
    NvmeGwCreated& gw_state = Created_gws[group_key][gw_id];
    epoch_t epoch;
    dout(4) << "Found failover GW " << gw_id << " for ANA group " << (int)ANA_groupid << dendl;
    int rc = blocklist_gw (failed_gw_id, group_key, ANA_groupid, epoch, true);
    if(rc){
        gw_state.active_state(ANA_groupid); //TODO check whether it is valid to  start failover when nonces are empty !
        //ceph_assert(false);
    }
    else{
        gw_state.sm_state[ANA_groupid] = GW_STATES_PER_AGROUP_E::GW_WAIT_BLOCKLIST_CMPL;
        gw_state.blocklist_data[ANA_groupid].osd_epoch = epoch;
        gw_state.blocklist_data[ANA_groupid].is_failover = true;
        start_timer(gw_id, group_key, ANA_groupid, 30); //start Failover preparation timer
    }
}

void  NVMeofGwMap::find_failback_gw(const NvmeGwId &gw_id, const NvmeGroupKey& group_key,   bool &propose)
{
    auto& gws_states = Created_gws[group_key];
    auto& gw_state = Created_gws[group_key][gw_id];
    bool do_failback = false;

    dout(4) << "Find failback GW for GW " << gw_id << dendl;
    for (auto& gw_state_it: gws_states) {
        auto& st = gw_state_it.second;
        if (st.sm_state[gw_state.ana_grp_id] != GW_STATES_PER_AGROUP_E::GW_STANDBY_STATE) {// some other gw owns or owned the desired ana-group
            do_failback = true;// if candidate is in state ACTIVE for the desired ana-group, then failback starts immediately, otherwise need to wait
            dout(4) << "Found some gw " << gw_state_it.first  <<  " in state " << st.sm_state[gw_state.ana_grp_id]  << dendl;
            break;
        }
    }

    if (do_failback == false) {
        // No other gw currently performs some activity with desired ana group of coming-up GW - so it just takes over on the group
        dout(4)  << "Failback GW candidate was not found, just set Optimized to group " << gw_state.ana_grp_id << " to GW " << gw_id << dendl;
        gw_state.active_state(gw_state.ana_grp_id);
        propose = true;
        return;
    }
    //try to do_failback
    for (auto& gw_state_it: gws_states) {
        auto& failback_gw_id = gw_state_it.first;
        auto& st = gw_state_it.second;
        if (st.sm_state[gw_state.ana_grp_id] == GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE) {
            dout(4)  << "Found Failback GW " << failback_gw_id << " that previously took over the ANAGRP " << gw_state.ana_grp_id << " of the available GW " << gw_id << dendl;
            st.sm_state[gw_state.ana_grp_id] = GW_STATES_PER_AGROUP_E::GW_WAIT_FAILBACK_PREPARED;
            start_timer(failback_gw_id, group_key, gw_state.ana_grp_id, 3);// Add timestamp of start Failback preparation
            gw_state.sm_state[gw_state.ana_grp_id] = GW_STATES_PER_AGROUP_E::GW_OWNER_WAIT_FAILBACK_PREPARED;
            propose = true;
            break;
        }
    }
}


// TODO When decision to change ANA state of group is prepared, need to consider that last seen FSM state is "approved" - means it was returned in beacon alone with map version
void  NVMeofGwMap::find_failover_candidate(const NvmeGwId &gw_id, const NvmeGroupKey& group_key, NvmeAnaGrpId grpid, bool &propose_pending)
{
    dout(4) <<__func__<< " " << gw_id << dendl;
    #define ILLEGAL_GW_ID " "
    #define MIN_NUM_ANA_GROUPS 0xFFF
    int min_num_ana_groups_in_gw = 0;
    int current_ana_groups_in_gw = 0;
    NvmeGwId min_loaded_gw_id = ILLEGAL_GW_ID;

    auto& gws_states = Created_gws[group_key];

    auto gw_state = gws_states.find(gw_id);
    ceph_assert(gw_state != gws_states.end());

    // this GW may handle several ANA groups and  for each of them need to found the candidate GW
    if (gw_state->second.sm_state[grpid] == GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE || gw_state->second.ana_grp_id == grpid) {

        for (auto& found_gw_state: gws_states) { // for all the gateways of the subsystem
            auto st = found_gw_state.second;
            if(st.sm_state[grpid] ==  GW_STATES_PER_AGROUP_E::GW_WAIT_BLOCKLIST_CMPL){   // some GW already started failover/failback on this group
               dout(4) << "Failover" << st.blocklist_data[grpid].is_failover <<  " already started for the group " << grpid <<  " by GW " << found_gw_state.first << dendl;
               gw_state->second.standby_state(grpid);
               return ;
            }
        }

        // Find a GW that takes over the ANA group(s)
        min_num_ana_groups_in_gw = MIN_NUM_ANA_GROUPS;
        min_loaded_gw_id = ILLEGAL_GW_ID;
        for (auto& found_gw_state: gws_states) { // for all the gateways of the subsystem
            auto st = found_gw_state.second;
            if (st.availability == GW_AVAILABILITY_E::GW_AVAILABLE) {
                current_ana_groups_in_gw = 0;
                for (int j = 0; j < MAX_SUPPORTED_ANA_GROUPS; j++) {
                    if (st.sm_state[j] == GW_STATES_PER_AGROUP_E::GW_OWNER_WAIT_FAILBACK_PREPARED || st.sm_state[j] == GW_STATES_PER_AGROUP_E::GW_WAIT_FAILBACK_PREPARED
                                                                                          || st.sm_state[j] == GW_STATES_PER_AGROUP_E::GW_WAIT_BLOCKLIST_CMPL){
                        current_ana_groups_in_gw = 0xFFFF;
                        break; // dont take into account   GWs in the transitive state
                    }
                    else if (st.sm_state[j] == GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE)
                        //dout(4) << " process GW down " << current_ana_groups_in_gw << dendl;
                        current_ana_groups_in_gw++; // how many ANA groups are handled by this GW
                    }

                    if (min_num_ana_groups_in_gw > current_ana_groups_in_gw) {
                        min_num_ana_groups_in_gw = current_ana_groups_in_gw;
                        min_loaded_gw_id = found_gw_state.first;
                        dout(4) << "choose: gw-id  min_ana_groups " << min_loaded_gw_id << current_ana_groups_in_gw << " min " << min_num_ana_groups_in_gw << dendl;
                    }
                }
            }
            if (min_loaded_gw_id != ILLEGAL_GW_ID) {
                propose_pending = true;
                set_failover_gw_for_ANA_group(gw_id, group_key, min_loaded_gw_id, grpid);
            }
            else {
                if (gw_state->second.sm_state[grpid] == GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE){// not found candidate but map changed.
                    propose_pending = true;
                    dout(4) << "gw down no candidate found " << dendl;
                }
            }
            gw_state->second.standby_state(grpid);
        }
}

void NVMeofGwMap::fsm_handle_gw_alive (const NvmeGwId &gw_id, const NvmeGroupKey& group_key,  NvmeGwCreated & gw_state, GW_STATES_PER_AGROUP_E state, NvmeAnaGrpId grpid, epoch_t& last_osd_epoch, bool &map_modified)
{
    switch (state) {
    case GW_STATES_PER_AGROUP_E::GW_WAIT_BLOCKLIST_CMPL:
    {
        int timer_val = get_timer(gw_id, group_key, grpid);
        NvmeGwCreated& gw_map = Created_gws[group_key][gw_id];
            if(gw_map.blocklist_data[grpid].osd_epoch <= last_osd_epoch){
                dout(4) << "is-failover: " << gw_map.blocklist_data[grpid].is_failover << " osd epoch changed from " << gw_map.blocklist_data[grpid].osd_epoch << " to "<< last_osd_epoch
                                       << " Ana-grp: " << grpid  << " timer:" << timer_val << dendl;
                gw_state.active_state(grpid);                   // Failover Gw still alive and guaranteed that
                cancel_timer(gw_id, group_key, grpid);          // ana group wouldnt be taken back  during blocklist wait period
                map_modified = true;
            }
            else{
                dout(20) << "osd epoch not changed from " <<  gw_map.blocklist_data[grpid].osd_epoch << " to "<< last_osd_epoch
                                                   << " Ana-grp: " << grpid  << " timer:" << timer_val << dendl;
            }
    }
    break;

    default:
        break;
    }
}

 void NVMeofGwMap::fsm_handle_gw_down(const NvmeGwId &gw_id, const NvmeGroupKey& group_key,   GW_STATES_PER_AGROUP_E state, NvmeAnaGrpId grpid,  bool &map_modified)
 {
    switch (state)
    {
        case GW_STATES_PER_AGROUP_E::GW_STANDBY_STATE:
        case GW_STATES_PER_AGROUP_E::GW_IDLE_STATE:
            // nothing to do
            break;

        case GW_STATES_PER_AGROUP_E::GW_WAIT_BLOCKLIST_CMPL:
        {
            cancel_timer(gw_id, group_key, grpid);
        }break;

        case GW_STATES_PER_AGROUP_E::GW_WAIT_FAILBACK_PREPARED:
            cancel_timer(gw_id, group_key,  grpid);

            for (auto& gw_st: Created_gws[group_key]) {
                auto& st = gw_st.second;
                if (st.sm_state[grpid] == GW_STATES_PER_AGROUP_E::GW_OWNER_WAIT_FAILBACK_PREPARED) { // found GW   that was intended for  Failback for this ana grp
                    dout(4) << "Warning: Outgoing Failback when GW is down back - to rollback it"  <<" GW "  <<gw_id << "for ANA Group " << grpid << dendl;
                    st.standby_state(grpid);
                    map_modified = true;
                    break;
                }
            }
            break;

        case GW_STATES_PER_AGROUP_E::GW_OWNER_WAIT_FAILBACK_PREPARED:
            // nothing to do - let failback timer expire
            break;

        case GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE:
        {
            find_failover_candidate( gw_id, group_key, grpid, map_modified);
        }
        break;

        default:{
            ceph_assert(false);
        }

    }
 }


void NVMeofGwMap::fsm_handle_gw_delete (const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
     GW_STATES_PER_AGROUP_E state , NvmeAnaGrpId grpid, bool &map_modified) {
    switch (state)
    {
        case GW_STATES_PER_AGROUP_E::GW_STANDBY_STATE:
        case GW_STATES_PER_AGROUP_E::GW_IDLE_STATE:
        case GW_STATES_PER_AGROUP_E::GW_OWNER_WAIT_FAILBACK_PREPARED:
        {
            NvmeGwCreated& gw_state = Created_gws[group_key][gw_id];

            if (grpid == gw_state.ana_grp_id) {// Try to find GW that temporary owns my group - if found, this GW should pass to standby for  this group
                auto& gateway_states = Created_gws[group_key];
                for (auto& gs: gateway_states) {
                    if (gs.second.sm_state[grpid] == GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE  || gs.second.sm_state[grpid] == GW_STATES_PER_AGROUP_E::GW_WAIT_FAILBACK_PREPARED){
                        gs.second.standby_state(grpid);
                        map_modified = true;
                        if (gs.second.sm_state[grpid] == GW_STATES_PER_AGROUP_E::GW_WAIT_FAILBACK_PREPARED)
                            cancel_timer(gs.first, group_key, grpid);
                        break;
                    }
                }
            }
        }
        break;

        case GW_STATES_PER_AGROUP_E::GW_WAIT_BLOCKLIST_CMPL:
        {
            NvmeGwCreated& gw_state = Created_gws[group_key][gw_id];
            cancel_timer(gw_id, group_key, grpid);
            map_modified = true;
            gw_state.standby_state(grpid);
        }
        break;

        case GW_STATES_PER_AGROUP_E::GW_WAIT_FAILBACK_PREPARED:
        {
            cancel_timer(gw_id, group_key, grpid);
            for (auto& nqn_gws_state: Created_gws[group_key]) {
                auto& st = nqn_gws_state.second;

                if (st.sm_state[grpid] == GW_STATES_PER_AGROUP_E::GW_OWNER_WAIT_FAILBACK_PREPARED) { // found GW   that was intended for  Failback for this ana grp
                    dout(4) << "Warning: Outgoing Failback when GW is deleted - to rollback it" << " GW " << gw_id << "for ANA Group " << grpid << dendl;
                    st.standby_state(grpid);
                    map_modified = true;
                    break;
                }
            }
        }
        break;

        case GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE:
        {
            NvmeGwCreated& gw_state = Created_gws[group_key][gw_id];
            map_modified = true;
            gw_state.standby_state(grpid);
        }
        break;

        default: {
            ceph_assert(false);
        }
    }
    if (map_modified) validate_gw_map(group_key);
}

void NVMeofGwMap::fsm_handle_to_expired(const NvmeGwId &gw_id, const NvmeGroupKey& group_key, NvmeAnaGrpId grpid,  bool &map_modified)
{
    auto& fbp_gw_state = Created_gws[group_key][gw_id];// GW in Fail-back preparation state fbp
    bool grp_owner_found = false;
    if (fbp_gw_state.sm_state[grpid] == GW_STATES_PER_AGROUP_E::GW_WAIT_FAILBACK_PREPARED) {
        for (auto& gw_state: Created_gws[group_key]) {
            auto& st = gw_state.second;
            if (st.ana_grp_id == grpid){// group owner
                grp_owner_found = true;
                if(st.availability == GW_AVAILABILITY_E::GW_AVAILABLE) {
                   if( ! (fbp_gw_state.last_gw_map_epoch_valid  && st.last_gw_map_epoch_valid) ){
                     //Timer is not cancelled so it would expire over and over as long as both gws are not updated
                     dout(1) << "gw " << gw_id  <<" or gw " << gw_state.first  << "map epochs are not updated "<< dendl;
                     return;
                   }
                }
                cancel_timer(gw_id, group_key, grpid);
                if (st.sm_state[grpid] == GW_STATES_PER_AGROUP_E::GW_OWNER_WAIT_FAILBACK_PREPARED && st.availability == GW_AVAILABILITY_E::GW_AVAILABLE )
                {
                    fbp_gw_state.standby_state(grpid);// Previous failover GW  set to standby
                    st.active_state(grpid);
                    dout(4)  << "Expired Failback-preparation timer from GW " << gw_id << " ANA groupId "<< grpid << dendl;
                    map_modified = true;
                    break;
                }
                else if(st.sm_state[grpid] == GW_STATES_PER_AGROUP_E::GW_STANDBY_STATE  &&  st.availability == GW_AVAILABILITY_E::GW_AVAILABLE) {
                   st.standby_state(grpid);// GW failed during the persistency interval
                   dout(4)  << "Failback unsuccessfull. GW: " << gw_state.first << " becomes Standby for the ANA groupId " << grpid  << dendl;
                }
                fbp_gw_state.standby_state(grpid);
                dout(4) << "Failback unsuccessfull GW: " << gw_id << " becomes Standby for the ANA groupId " << grpid  << dendl;
                map_modified = true;
                break;
            }
       }
      ceph_assert(grp_owner_found);// when  GW group owner is deleted the fbk gw is put to standby
    }
    else if(fbp_gw_state.sm_state[grpid] == GW_STATES_PER_AGROUP_E::GW_WAIT_BLOCKLIST_CMPL){
        dout(1) << " Expired GW_WAIT_FAILOVER_PREPARED timer from GW " << gw_id << " ANA groupId: "<< grpid << dendl;
        ceph_assert(false);
    }
    if (map_modified) validate_gw_map(group_key);
}

NvmeGwCreated& NVMeofGwMap::find_already_created_gw(const NvmeGwId &gw_id, const NvmeGroupKey& group_key)
{
    auto& group_gws = Created_gws[group_key];
    auto  gw_it = group_gws.find(gw_id);
    ceph_assert(gw_it != group_gws.end());//should not happen
    return gw_it->second;
}

struct CMonRequestProposal : public Context {
  NVMeofGwMap *m;
  entity_addrvec_t addr_vect;
  utime_t expires;
  CMonRequestProposal(NVMeofGwMap *mon , entity_addrvec_t addr_vector, utime_t until) : m(mon), addr_vect(addr_vector), expires (until)  {}
  void finish(int r) {
      dout(4) << "osdmon is  writable? " << m->mon->osdmon()->is_writeable() << dendl;
      if(m->mon->osdmon()->is_writeable()){
        epoch_t epoch = m->mon->osdmon()->blocklist(addr_vect, expires);
        dout (4) << "epoch " << epoch <<dendl;
        m->mon->nvmegwmon()->request_proposal(m->mon->osdmon());
      }
      else {
          m->mon->osdmon()->wait_for_writeable_ctx( new CMonRequestProposal(m, addr_vect, expires));
      }
  }
};

int NVMeofGwMap::blocklist_gw(const NvmeGwId &gw_id, const NvmeGroupKey& group_key, NvmeAnaGrpId grpid, epoch_t &epoch, bool failover)
{
    NvmeGwCreated& gw_map =  Created_gws[group_key][gw_id];  //find_already_created_gw(gw_id, group_key);

     if (gw_map.nonce_map[grpid].size() > 0){
        NvmeNonceVector &nonce_vector = gw_map.nonce_map[grpid];;
        std::string str = "[";
        entity_addrvec_t addr_vect;

        double d = g_conf().get_val<double>("mon_osd_blocklist_default_expire");
        utime_t expires = ceph_clock_now();
        expires += d;
        dout(4) << " blocklist timestamp " << expires << dendl;
        for(auto &it: nonce_vector ){
            if(str != "[") str += ",";
            str += it;
        }
        str += "]";
        bool rc = addr_vect.parse(&str[0]);
        dout(10) << str << " rc " << rc <<  " network vector: " << addr_vect << " " << addr_vect.size() << dendl;
        ceph_assert(rc);

        epoch = mon->osdmon()->blocklist(addr_vect, expires);

        if (!mon->osdmon()->is_writeable()) {
            dout(4) << "osdmon is not writable, waiting, epoch = " << epoch << dendl;
            mon->osdmon()->wait_for_writeable_ctx( new CMonRequestProposal(this, addr_vect, expires ));// return false;
        }
        else  mon->nvmegwmon()->request_proposal(mon->osdmon());
        dout(4) << str << " mon->osdmon()->blocklist: epoch : " << epoch <<  " address vector: " << addr_vect << " " << addr_vect.size() << dendl;
    }
    else{
        dout(1) << "Error: No nonces context present for gw: " <<gw_id  << " ANA group: " << grpid << dendl;
        return 1;
    }
    return 0;
}

void  NVMeofGwMap::validate_gw_map(const NvmeGroupKey& group_key)
{
   NvmeAnaGrpId anas[MAX_SUPPORTED_ANA_GROUPS];
   int i = 0;
   int max_groups = 0;
   for (auto& gw_created_pair: Created_gws[group_key]) {
        auto& st = gw_created_pair.second;
        anas[i++] = st.ana_grp_id;
   }
   max_groups = i;
   for(int i = 0; i < max_groups; i++)
   {
       int ana_group = anas[i];
       int count = 0;
       for (auto& gw_created_pair: Created_gws[group_key]) {
           auto& st = gw_created_pair.second;
           if (st.sm_state[ana_group] == GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE){
               count ++;
               if(count == 2) {
                   dout(1) << "number active states per ana-group " << ana_group << "more than 1 in pool-group " << group_key << dendl;
                   dout(1) << Created_gws[group_key] << dendl;
                   ceph_assert(false);
               }
           }
       }
   }
}

void NVMeofGwMap::update_active_timers( bool &propose_pending ){

    //dout(4) << __func__  <<  " called,  p_monitor: " << mon << dendl;
    const auto now = std::chrono::system_clock::now();
    for (auto& group_md: Gmetadata) {
        auto& group_key = group_md.first;
        auto& pool = group_key.first;
        auto& group = group_key.second;
        for (auto& gw_md: group_md.second) {
            auto& gw_id = gw_md.first;
            auto& md = gw_md.second;
            for (size_t ana_grpid = 0; ana_grpid < MAX_SUPPORTED_ANA_GROUPS; ana_grpid ++) {
                if (md.data[ana_grpid].timer_started == 0) continue;
                dout(20) << "Checking timer for GW " << gw_id << " ANA GRP " << ana_grpid<< " value(seconds): "<< (int)md.data[ana_grpid].timer_value << dendl;
                if(now >= md.data[ana_grpid].end_time){
                    fsm_handle_to_expired (gw_id, std::make_pair(pool, group), ana_grpid, propose_pending);
                }
            }
        }
    }
}


void NVMeofGwMap::start_timer(const NvmeGwId &gw_id, const NvmeGroupKey& group_key, NvmeAnaGrpId anagrpid, uint8_t value_sec) {
    Gmetadata[group_key][gw_id].data[anagrpid].timer_started = 1;
    Gmetadata[group_key][gw_id].data[anagrpid].timer_value = value_sec;
    dout(4) << "start timer for ana " << anagrpid << " gw " << gw_id << "value sec " << (int)value_sec << dendl;
    const auto now = std::chrono::system_clock::now();
    Gmetadata[group_key][gw_id].data[anagrpid].end_time = now + std::chrono::seconds(value_sec);
}

int  NVMeofGwMap::get_timer(const NvmeGwId &gw_id, const NvmeGroupKey& group_key, NvmeAnaGrpId anagrpid) {
    auto timer = Gmetadata[group_key][gw_id].data[anagrpid].timer_value;
    return timer;
}

void NVMeofGwMap::cancel_timer(const NvmeGwId &gw_id, const NvmeGroupKey& group_key, NvmeAnaGrpId anagrpid) {
    Gmetadata[group_key][gw_id].data[anagrpid].timer_started = 0;
}

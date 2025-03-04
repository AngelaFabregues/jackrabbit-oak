/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.AutoMembershipConfig;
import org.apache.jackrabbit.oak.spi.security.principal.GroupPrincipals;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

final class AutoMembershipPrincipals {

    private static final Logger log = LoggerFactory.getLogger(AutoMembershipPrincipals.class);

    private final UserManager userManager;
    private final Map<String, String[]> autoMembershipMapping;
    private final Map<String, AutoMembershipConfig> autoMembershipConfigMap;
    private final Map<String, Set<Principal>> principalMap;

    AutoMembershipPrincipals(@NotNull UserManager userManager,
                             @NotNull Map<String, String[]> autoMembershipMapping,
                             @NotNull Map<String, AutoMembershipConfig> autoMembershipConfigMap) {
        this.userManager = userManager;
        this.autoMembershipMapping = autoMembershipMapping;
        this.autoMembershipConfigMap = autoMembershipConfigMap;
        this.principalMap = new ConcurrentHashMap<>(autoMembershipMapping.size());
    }
    
    /**
     * Return the IDP names for which the given group principal is configured in the global auto-membership configuration.
     * 
     * @param groupPrincipal A group principal
     * @return A set of IDP names.
     * @see AutoMembershipProvider#getMembers(Group, boolean) 
     */
    Set<String> getConfiguredIdpNames(@NotNull Principal groupPrincipal) {
        // populate principal-map for all IDP-names in the mapping
        if (principalMap.isEmpty() && !autoMembershipMapping.isEmpty()) {
            for (String idpName : autoMembershipMapping.keySet()) {
                collectGlobalAutoMembershipPrincipals(idpName);
            }
        }
        
        String name = groupPrincipal.getName();
        Set<String> idpNames = new HashSet<>(principalMap.size());
        principalMap.forEach((idpName, principals) -> {
            if (principals.stream().anyMatch(principal -> name.equals(principal.getName()))) {
                idpNames.add(idpName);
            }
        });
        return idpNames;
    }

    /**
     * Returns the automatic members of the given group across all configured IDPs according to the result of  
     * {@link AutoMembershipConfig#getAutoMembers(UserManager, Group)}
     * 
     * @param group The target group
     * @return An iterator of users/groups that have the given group as auto-membership group defined in {@link AutoMembershipConfig#getAutoMembership(Authorizable)}
     * @see AutoMembershipProvider#getMembers(Group, boolean) 
     */
    @NotNull
    Iterator<Authorizable> getMembersFromAutoMembershipConfig(@NotNull Group group) {
        List<Iterator<? extends Authorizable>> results = new ArrayList<>();
        autoMembershipConfigMap.values().forEach(autoMembershipConfig -> results.add(autoMembershipConfig.getAutoMembers(userManager, group)));
        return Iterators.concat(results.iterator());
    }

    /**
     * Tests if the given authorizabe is an automatic member of the group identified by {@code groupId}. Note that this 
     * method evaluates both global auto-membership mapping and {@link AutoMembershipConfig} if they exist for the given IDP name.
     * 
     * @param idpName The name of an IDP
     * @param groupId The target group id
     * @param authorizable The authorizable for which to evaluation if it is a automatic member of the group identified by {@code groupId}.
     * @return {@code true} if the given authorizable is an automatic member of the group identified by {@code groupId}; {@code false} otherwise.
     * @see AutoMembershipProvider#isMember(Group, Authorizable, boolean) 
     */
    boolean isMember(@NotNull String idpName, @NotNull String groupId, @NotNull Authorizable authorizable) {
        // check global auto-membership mapping first  
        String[] vs = autoMembershipMapping.get(idpName);
        if (vs != null) {
            for (String grId : vs) {
                if (groupId.equals(grId)) {
                    return true;
                }
            }
        }

        // if not defined in global mapping test if any match is found in the auto-membership configurations
        AutoMembershipConfig config = autoMembershipConfigMap.get(idpName);
        if (config != null) {
            return config.getAutoMembership(authorizable).contains(groupId);
        }
        return false;
    }

    /**
     * Returns the group principal that given authorizable is an automatic member of. This method evaluates both the 
     * global auto-membership settings as well as {@link AutoMembershipConfig} if they exist for the given IDP name.
     * 
     * @param idpName The name of an IDP
     * @param authorizable The target user/group
     * @return A collection of principals the given authorizable is an automatic member of.
     */
    @NotNull
    Collection<Principal> getAutoMembership(@NotNull String idpName, @NotNull Authorizable authorizable) {
        ImmutableSet.Builder<Principal> builder = ImmutableSet.builder();
        
        // global auto-membership
        builder.addAll(collectGlobalAutoMembershipPrincipals(idpName));
        
        // conditional auto-membership
        AutoMembershipConfig config = autoMembershipConfigMap.get(idpName);
        if (config != null) {
            config.getAutoMembership(authorizable).forEach(groupId -> addVerifiedPrincipal(groupId, builder));
        }
        return builder.build();
    }
    
    @NotNull
    private Set<Principal> collectGlobalAutoMembershipPrincipals(@NotNull String idpName) {
        if (!principalMap.containsKey(idpName)) {
            ImmutableSet.Builder<Principal> builder = ImmutableSet.builder();
            String[] vs = autoMembershipMapping.get(idpName);
            if (vs != null) {
                for (String groupId : vs) {
                    addVerifiedPrincipal(groupId, builder);
                }
            }
            Set<Principal> principals = builder.build();
            principalMap.put(idpName, principals);
            return principals;
        } else {
            return principalMap.get(idpName);
        }
    }
    
    private void addVerifiedPrincipal(@NotNull String groupId, @NotNull ImmutableSet.Builder<Principal> builder) {
        Principal principal = getVerifiedPrincipal(groupId);
        if (principal != null) {
            builder.add(principal);
        }
    }
    
    @Nullable
    private Principal getVerifiedPrincipal(@NotNull String groupId) {
        try {
            Authorizable gr = userManager.getAuthorizable(groupId);
            if (gr != null && gr.isGroup()) {
                Principal grPrincipal = gr.getPrincipal();
                if (GroupPrincipals.isGroup(grPrincipal)) {
                    return grPrincipal;
                } else {
                    log.warn("Principal of group {} is not of group type -> Ignoring", groupId);
                }
            } else {
                log.warn("Configured auto-membership group {} does not exist -> Ignoring", groupId);
            }
        } catch (RepositoryException e) {
            log.debug("Failed to retrieved 'auto-membership' group with id {}", groupId, e);
        }
        return null;
    }
}

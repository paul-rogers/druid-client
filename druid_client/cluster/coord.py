# Copyright 2022 Paul Rogers
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ..client.service import Service
from ..client import consts
from ..client.util import encode_interval
from ..client import error

COORD_BASE = '/druid/coordinator/v1'

# Leadership
REQ_COORD_LEADER = COORD_BASE + '/leader'
REQ_IS_COORD_LEADER = COORD_BASE + '/isLeader'

# Segment load status
REQ_LOAD_STATUS = COORD_BASE + '/loadstatus'
REQ_LOAD_QUEUE = COORD_BASE + '/loadqueue'

# Compaction
REQ_COMPACTION = COORD_BASE + '/compaction'
REQ_COMPACTION_STATUS = REQ_COMPACTION + '/status'
REQ_COMPACTION_PROGRESS = REQ_COMPACTION + '/progress'

# Dynamic configuration
REQ_DYNAMIC_CONFIG = COORD_BASE + '/config'

# Retention rules
REQ_RET_RULES = COORD_BASE + '/rules'
REQ_RET_HISTORY = REQ_RET_RULES + '/history'
REQ_DS_RET_RULES = REQ_RET_RULES + '/{}'
REQ_DS_RET_HISTORY = REQ_DS_RET_RULES + '/history'

# Datasources
DATA_SOURCE_BASE = COORD_BASE + '/datasources'
REQ_DATASOURCES = DATA_SOURCE_BASE
REQ_DS_PROPERTIES = REQ_DATASOURCES + '/{}'
REQ_DROP_DS = REQ_DS_PROPERTIES

# Metadata
METADATA_BASE = COORD_BASE + '/metadata'
REQ_MD_DATASOURCES = METADATA_BASE + '/datasources'
REQ_MD_DS_DETAILS = REQ_MD_DATASOURCES + '/{}'
REQ_MD_SEGMENTS = REQ_MD_DS_DETAILS + '/segments'
REQ_MD_SEGMENT_DETAILS = REQ_MD_SEGMENTS + '/{}'
REQ_INTERVALS = REQ_DS_PROPERTIES + '/intervals'
REQ_INTERVAL = REQ_INTERVALS + '/{}'
REQ_INTERVAL_SERVERS = REQ_INTERVAL + '/serverview'
REQ_SEGMENTS = REQ_DS_PROPERTIES + '/segments'
REQ_SEGMENT_DETAILS = REQ_MD_SEGMENTS + '/{}'
REQ_DS_TIERS = REQ_DS_PROPERTIES + '/tiers'

# Cluster membership
REQ_CLUSTER = COORD_BASE + "/cluster"

# Lookups
LOOKUP_BASE = COORD_BASE + '/lookups/config'
REQ_LU_CONFIG = LOOKUP_BASE
REQ_LU_ALL_LOOKUPS = REQ_LU_CONFIG + '/all'
REQ_LU_TIER_CONFIG = LOOKUP_BASE + '/{}'
REQ_LU_LOOKUP_CONFIG = REQ_LU_TIER_CONFIG + '/{}'


class Coordinator(Service):
    """
    Represents a Druid Coordinator.

    Deprecated APIs
    ---------------
    As Druid has evolved, the API functionality has changed. This client does not
    implement the now-obsolete APIs. Druid recommends issuing SQL queries against
    the system tables instead.

    * `/druid/coordinator/v1/metadata/segments`
    * `/druid/coordinator/v1/metadata/segments?datasources={dataSourceName1}...`
    * `/druid/coordinator/v1/metadata/segments?includeOvershadowedStatus`
    * `/druid/coordinator/v1/metadata/segments?includeOvershadowedStatus&datasources={dataSourceName1}...`
    * `/druid/coordinator/v1/datasources/{dataSourceName}/segments`
    * `/druid/coordinator/v1/datasources/{dataSourceName}/segments?full`
    * `/druid/coordinator/v1/datasources/{dataSourceName}/segments/{segmentId}`
    * `/druid/coordinator/v1/servers` (see sys.servers table)
    * `/druid/coordinator/v1/servers?simple` (see sys.servers table)

    APIs Not Yet Implemented
    ------------------------

    * `POST /druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments`
    * `POST /druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments?full`
    * `POST /druid/coordinator/v1/datasources/{dataSourceName}/markUsed`
    * `POST /druid/coordinator/v1/datasources/{dataSourceName}/markUnused`
    * `DELETE /druid/coordinator/v1/datasources/{dataSourceName}/intervals/{interval}`
    * `DELETE /druid/coordinator/v1/datasources/{dataSourceName}/segments/{segmentId}`
    * Retention rules
    * Intervals
    * Dynamic configuration
    * Compaction Status
    * Compaction Configuration
    * Minor Lookup APIs: https://druid.apache.org/docs/latest/querying/lookups.html
    """

    def __init__(self, cluster_config, endpoint):
        Service.__init__(self, cluster_config, endpoint)
    
    def service(self):
        return consts.COORDINATOR
   
    #-------- Leadership --------

    def lead(self) -> str:
        """
        Returns the current leader Coordinator of the cluster.

        See https://druid.apache.org/docs/latest/operations/api-reference.html#leadership
        """
        return self.get(REQ_COORD_LEADER).text
    
    def is_lead(self) -> bool:
        """
        Returns True if this server is the current leader Coordinator of the cluster,
        False otherwise.

        See https://druid.apache.org/docs/latest/operations/api-reference.html#leadership
        """
        body = self.get_json(REQ_IS_COORD_LEADER)
        return body["leader"]
   
    #-------- Segment loading status --------
    
    def load_status_percent(self):
        """
        Returns the percentage of segments actually loaded in the cluster versus 
        segments that should be loaded in the cluster.

        Reference
        ---------
        `GET /druid/coordinator/v1/loadstatus`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#segment-loading
        """
        return self.get_json(REQ_LOAD_STATUS)
    
    def load_status_count(self):
        """
        Returns the number of segments left to load until segments that should 
        be loaded in the cluster are available for queries. This does not include 
        segment replication counts.

        Reference
        ---------
        `GET /druid/coordinator/v1/loadstatus?simple`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#segment-loading
        """
        return self.get_json(REQ_LOAD_STATUS, params={'simple': ''})
    
    def load_status_full(self):
        """
        Returns the number of segments left to load in each tier until segments 
        that should be loaded in the cluster are all available. This includes 
        segment replication counts.

        Reference
        ---------
        `GET /druid/coordinator/v1/loadstatus?full`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#segment-loading
        """
        return self.get_json(REQ_LOAD_STATUS, params={"full": ''})
    
    def load_status_by_tier(self):
        """
        Returns the number of segments not yet loaded for each tier until all segments 
        loading in the cluster are available.
        
        The result includes segment replication counts. It also factors in the number of 
        available nodes that are of a service type that can load the segment when computing 
        the number of segments remaining to load. A segment is considered fully loaded when:

        * Druid has replicated it the number of times configured in the corresponding load rule.
        * Or the number of replicas for the segment in each tier where it is configured to be 
          replicated equals the available nodes of a service type that are currently allowed
          to load the segment in the tier.

        The actual return value is load status by node, not by tier as described above
        (which is taken from the Druid documentation.)

        Reference
        ---------
        `GET /druid/coordinator/v1/loadstatus?full&computeUsingClusterView`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#segment-loading
        """
        return self.get_json(REQ_LOAD_QUEUE, params={'full': '', 'computeUsingClusterView': ''})
    
    def load_queue(self):
        """
        Returns the ids of segments to load and drop for each Historical process.

        Reference
        ---------
        `GET /druid/coordinator/v1/loadqueue`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#segment-loading
        """
        return self.get_json(REQ_LOAD_QUEUE)
    
    def load_queue_simple(self):
        """
        Returns the number of segments to load and drop, as well as the total 
        segment load and drop size in bytes for each Historical process.

        Reference
        ---------
        `GET /druid/coordinator/v1/loadqueue?simple`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#segment-loading
        """
        return self.get_json(REQ_LOAD_QUEUE, params={"simple": ''})
    
    def load_queue_full(self):
        """
        Returns the segments to load and drop for each Historical process.

        Reference
        ---------
        `GET`/druid/coordinator/v1/loadqueue?full`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#segment-loading
        """
        return self.get_json(REQ_LOAD_QUEUE, params={"full": ''})

    #-------- Compaction status --------

    def post_compaction_config(self, config):
        """
        Creates or updates the compaction config for a dataSource.

        Reference
        ---------
        `POST /druid/coordinator/v1/config/compaction`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#post-4
        See https://druid.apache.org/docs/latest/configuration/index.html#compaction-dynamic-configuration
        """

    def compaction_configs(self):
        """
        Returns all compaction configs.

        Reference
        ---------
        `GET /druid/coordinator/v1/config/compaction`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#get-12
        """
        return self.get_json(REQ_COMPACTION)

    def compaction_config_for(self, data_source):
        """
        Returns all compaction configs.

        Reference
        ---------
        `GET /druid/coordinator/v1/config/compaction`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#get-12
        """
        return self.get_json(REQ_COMPACTION, params={'dataSource': data_source})

    def compaction_status(self):
        """
        Returns the status and statistics from the auto compaction run of all 
        dataSources which have auto compaction enabled in the latest run.

        Reference
        ---------
        `GET /druid/coordinator/v1/compaction/status`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#compaction-status
        """
        return self.get_json(REQ_COMPACTION_STATUS)

    def compaction_status_for(self, data_source):
        """
        Returns the status and statistics from the auto compaction run of the 
        specified data source.

        Reference
        ---------
        `GET /druid/coordinator/v1/compaction/status?dataSource={dataSource}`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#compaction-status
        """
        return self.get_json(REQ_COMPACTION_STATUS, params={'dataSource': data_source})

    def compaction_progress_for(self, data_source):
        """
        Returns the total size of segments awaiting compaction for the given dataSource.
        
        This is only valid for dataSource which has compaction enabled.

        Reference
        ---------
        `GET /druid/coordinator/v1/compaction/progress?dataSource={dataSource}`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#compaction-status
        """
        return self.get_json(REQ_COMPACTION_PROGRESS, params={'dataSource': data_source})

    #-------- Dynamic configuration --------

    def dynamic_config(self):
        """
        Retrieves current coordinator dynamic configuration.

        Reference
        ---------
        `GET /druid/coordinator/v1/config`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#dynamic-configuration
        """
        return self.get_json(REQ_DYNAMIC_CONFIG)

    #-------- Retention Rules --------

    def retention_rules(self):
        """
        Returns all rules as objects for all datasources in the cluster 
        including the default datasource.

        `GET /druid/coordinator/v1/rules`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#retention-rules
        """
        return self.get_json(REQ_RET_RULES)
    
    def audit_history(self, count=25):
        """
        Returns he most recent entries of audit history of rules for all datasources.

        Parameters
        ----------
        count : int, default = 25
            Number of audit entries to return

        Reference
        ---------
        `GET /druid/coordinator/v1/rules/history?count={n}

        See https://druid.apache.org/docs/latest/operations/api-reference.html#retention-rules
        """
        # TODO: interval
        return self.get_json(REQ_RET_HISTORY, params={'count': str(count)})
    
    def retention_rules_for(self, data_source, full=False):
        """
        Returns retention all rules for a specified datasource.

        Parameters
        ----------
        data_source: str
            Name of the datasource to query
        
        full: bool, default = False
            Whether to include the default datasource.

        Reference
        ---------
        `GET /druid/coordinator/v1/rules/{dataSourceName}[?full]`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#retention-rules
        """
        params = {'full': ''} if full else None
        return self.get_json(REQ_DS_RET_RULES, args=[data_source], params=params)

    def audit_history_for(self, data_source, count=25):
        """
        Returns last entries of audit history of rules for a specified datasource.

        Parameters
        ----------
        data_source: str
            Name of the datasource to query.

        count : int, default = 25
            Number of audit entries to return.

        Reference
        ---------
        `GET /druid/coordinator/v1/rules/{dataSourceName}/history?count={n}`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#retention-rules
        """
        # TODO: interval
        return self.get_json(REQ_DS_RET_HISTORY, args=[data_source], params={'count': str(count)})

    #-------- Data Source Metadata --------
    
    def data_source_names(self, include_unused=False, include_disabled=False):
        """
        Returns a list of the names of data sources in the metadata store.
        
        Parameters
        ----------
        include_unused : bool, default = False
            if False, returns only datasources with at least one used segment
            in the cluster.

        include_unused : bool, default = False
            if False, returns only enamed datasources.

        Reference
        ---------
        * `GET /druid/coordinator/v1/metadata/datasources`
        * `GET /druid/coordinator/v1/metadata/datasources?includeUnused`
        * `GET /druid/coordinator/v1/metadata/datasources?includeDisabled`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#get-4

        Note: this method uses a semi-deprecated API.
        See `Metadata.user_table_names()` for the preferred solution.
        """
        params = {}
        if include_unused:
            params['includeUnused'] = ''
        if include_disabled:
            params['includeDisabled'] = ''
        return self.get_json(REQ_MD_DATASOURCES, params=params)
    
    def data_source_details(self):
        """
        Returns a list of all data sources with at least one used segment in 
        the cluster. Returns all metadata about those data sources as stored 
        in the metadata store.

        Reference
        ---------
        `GET /druid/coordinator/v1/metadata/datasources?full`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#get-4
        """
        return self.get_json(REQ_MD_DATASOURCES, params={"full": ""})

    def details_for_data_source(self, ds_name):
        """
        Returns full metadata for a datasource as stored in the metadata store.
        
        Parameters
        ----------
        ds_name: str
            name of the datasource to query

        Reference
        ---------
         `/druid/coordinator/v1/metadata/datasources/{dataSourceName}`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#get-4
        """
        return self.get_json(REQ_MD_DS_DETAILS, args=[ds_name])

    def segments_for_data_source(self, ds_name, full=False):
        """
        Returns a list of all segments for a datasource as stored in the metadata store.
        
        Parameters
        ----------
        ds_name: str
            name of the datasource to query
        full: bool, default is False
            includes the full segment metadata as stored in the metadata store.
   
        Reference
        ---------
        * `GET /druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments`
        * `GET /druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments?full`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#get-4

        This API is mostly replaced by the `sys.segments` system table.
        """
        params = None
        if full:
            params = {"full": ""}
        return self.get_json(REQ_MD_SEGMENTS, args=[ds_name], params=params)

    def segment_metadata(self, ds_name, segment_id):
        """
        Returns full segment metadata for a specific segment as stored in the metadata store.
       
        Parameters
        ----------
        ds_name: str
            name of the datasource to query
        segment_id: str
            id of the segment as returned from md_segments(). This method handles
            encoding into the form required by the API.

        Reference
        ---------
        `GET /druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments/{segmentId}`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#get-4

        This API is mostly replaced by the `sys.segments` system table.
        """
        return self.get_json(REQ_MD_SEGMENT_DETAILS, args=[ds_name, segment_id])

    def data_source_properties(self, full=False):
        """
        Returns a list of objects containing the name and properties of datasources 
        found in the cluster. Properties include segment count, total segment byte 
        size, replicated total segment byte size, minTime, and maxTime.

        Parameters
        ----------
        full : bool, default = False
            Whether to include the full set of details.

        Reference
        ---------
        * `GET /druid/coordinator/v1/datasources`
        * `GET /druid/coordinator/v1/datasources?full`

        This API is mostly replaced by the `sys.segments` system table.
        """
        params = None
        if full:
            params = {"full": ""}
        return self.get_json(REQ_DATASOURCES, params=params)
    
    def data_source_properties_for(self, ds_name, full=False):
        """
        Returns an object containing the name and properties of a datasource. 
        Properties include segment count, total segment byte size, replicated 
        total segment byte size, minTime, and maxTime.
       
        Parameters
        ----------
        ds_name: str
            name of the datasource to query
        full : bool, default = False
            Whether to include the full set of details.

        Reference
        ---------
        * `GET /druid/coordinator/v1/datasources/{dataSourceName}`
        * `GET /druid/coordinator/v1/datasources/{dataSourceName}?full`

        This API is mostly replaced by the `sys.segments` system table.
        """
        params = None
        if full:
            params = {"full": ""}
        return self.get_json(REQ_DS_PROPERTIES, args=[ds_name], params=params)
    
    def intervals(self, ds_name, option=None):
        """
        Internal method which information about intervals for a datasource.

        Parameters
        ----------
        ds_name: str
            name of the datasource to query
        option: str, default is None
            None: Returns a set of segment intervals. Each is a string in the format
                2012-01-01T00:00:00.000/2012-01-03T00:00:00.000.
            "simple": Returns a map of an interval to an object containing the total 
                byte size of segments and number of segments for that interval.
            "full": Returns a map of an interval to a map of segment metadata to a 
                 set of server names that contain the segment for that interval.

        Reference
        ---------
        * `GET /druid/coordinator/v1/datasources/{dataSourceName}/intervals`
        * `GET /druid/coordinator/v1/datasources/{dataSourceName}/intervals?simple`
        * `GET /druid/coordinator/v1/datasources/{dataSourceName}/intervals?full`
        """
        if option is None:
            params = None
        else:
            params = {option: ""}
        return self.get_json(REQ_INTERVALS, args=[ds_name], params=params)

    def interval(self, ds_name, interval_id, option=None):
        """
        Internal method which information about one interval for a datasource.

        Parameters
        ----------
        ds_name: str
            name of the datasource to query
        interval_id : str
            interval ID as returned from intervals()
        option: str, default is None
            None: Returns a set of segment ids for an interval.
            "simple": Returns a map of segment intervals contained within the specified interval to 
                an object containing the total byte size of segments and number of segments 
                for an interval.
             "full": Returns a map of segment intervals contained within the specified interval 
                to a map of segment metadata to a set of server names that contain the 
                segment for an interval.

        Reference
        ---------
        * `GET /druid/coordinator/v1/datasources/{dataSourceName}/intervals/{interval}`
        * `GET /druid/coordinator/v1/datasources/{dataSourceName}/intervals/{interval}?simple`
        * `GET /druid/coordinator/v1/datasources/{dataSourceName}/intervals/{interval}?full`
        """
        enc = encode_interval(interval_id)
        if option is None:
            params = None
        else:
            params = {option: ""}
        return self.get_json(REQ_INTERVAL, args=[ds_name, enc], params=params)

    def interval_servers(self, ds_name, interval_id):
        """
        Returns a map of segment intervals contained within the specified interval to information
        about the servers that contain the segment for an interval.
       
        Parameters
        ----------
        ds_name: str
            name of the datasource to query
        interval_id : str
            interval ID as returned from intervals()

        Reference
        ---------
        `GET /druid/coordinator/v1/datasources/{dataSourceName}/intervals/{interval}/serverview`
        """
        enc = encode_interval(interval_id)
        return self.get_json(REQ_INTERVAL_SERVERS, args=[ds_name, enc])

    def tiers_for(self, ds_name):
        """
        Return the tiers that a datasource exists in.
       
        Parameters
        ----------
        ds_name: str
            name of the datasource to query

        Reference
        ---------
        `GET /druid/coordinator/v1/datasources/{dataSourceName}/tiers`
        """
        return self.get_json(REQ_DS_TIERS, args=[ds_name])

    def drop_data_source(self, ds_name):
        """
        Drops a data source.

        Marks as unused all segments belonging to a datasource. 

        Marking all segments as unused is equivalent to dropping the table.
        
        Parameters
        ----------
        ds_name: str
            name of the datasource to query

        Returns
        -------
        Returns a map of the form 
        {"numChangedSegments": <number>} with the number of segments in the database whose 
        state has been changed (that is, the segments were marked as unused) as the result 
        of this API call.

        Reference
        ---------
        `DELETE /druid/coordinator/v1/datasources/{dataSourceName}`
        """
        return self.delete_json(REQ_DROP_DS, args=[ds_name])
   
    #-------- Lookups --------

    def initialize_lookups(self):
        """
        If you have NEVER configured lookups before, you MUST call this method
        to initialize the configuration.

        Reference
        ---------
        `POST /druid/coordinator/v1/lookups/config`

        See https://druid.apache.org/docs/latest/querying/lookups.html
        """
        obj = {
            consts.DEFAULT_TIER: {}
        }
        return self.post_json(REQ_LU_CONFIG, obj)
    
    def lookup_tiers(self, dynamic=False):
        """
        Returns a list of known tier names. Returns an empty
        list if lookups are not get initialized.
       
        Parameters
        ----------
        dynamic : bool, default = False
            Discovers a list of tiers currently active in the cluster in 
            addition to ones known in the dynamic configuration.

        Reference
        ---------
        `POST /druid/coordinator/v1/lookups/config`
        
        See https://druid.apache.org/docs/latest/querying/lookups.html

        See initialize_lookups()
        """
        params = None
        if dynamic:
            params = {'discover': 'true'}
        try:
            return self.get_jsonl(REQ_LU_CONFIG, params=params)
        except Exception as e:
            return []

    def all_lookups(self):
        """
        Returns all known lookup specs for all tiers in bulk update format,
        which is a dictionary of tiers which contain a dictionary of lookups.
        Returns an empty dictionary if lookups are not get initialized.

        Reference
        ---------
        `GET /druid/coordinator/v1/lookups/config/all`

        See initialize_lookups()

        See the bulk update format at
        https://druid.apache.org/docs/latest/querying/lookups.html#bulk-update
        """
        try:
            return self.get_json(REQ_LU_ALL_LOOKUPS)
        except Exception as e:
            return {}

    def lookups_for_tier(self, tier=consts.DEFAULT_TIER):
        """
        Returns a list of lookup names for the given tier.

        Parameters
        ----------
        tier: str, default = DEFAULT_TIER
            Name of the tier to query.

        Raises NotFoundError if the tier is undefined or lookups
        are not initialized.

        Reference
        ---------
        `GET /druid/coordinator/v1/lookups/config/{tier}`
        
        See https://druid.apache.org/docs/latest/querying/lookups.html
        """
        try:
            return self.get_json(REQ_LU_TIER_CONFIG, args=[tier])
        except Exception as e:
            raise error.NotFoundError("tier = " + tier)
        
    def lookup(self, tier, id, detailed=False):
        """
        Returns the lookup spec for the given id and tier.

        Parameters
        ----------
        tier: str
            Name of the tier to query.
        id: str
            id of the lookup assigned at creation or returned from
            lookups_for_tier()
        detailed: bool, default = False
            Includes additional detail.

        Raises NotFoundError if the tier is undefined, the lookup id
        is undefined or if lookups are not initialized.

        Reference
        ---------
        `GET /druid/coordinator/v1/lookups/config/{tier}/{id}`
        
        See https://druid.apache.org/docs/latest/querying/lookups.html
        """
        if detailed:
            params = {'detailed': ''}
        else:
            params = None
        try:
            return self.get_json(REQ_LU_LOOKUP_CONFIG, args=[tier, id], params=params)
        except Exception as e:
            raise error.NotFoundError("tier = {}, lookup id = {}".format(tier, id))
    
    def update(self, tier, id, defn):
        """
        Create or replace a lookup in the given tier. If updating, the
        version number of the new lookup must be greater than that of
        the existign lookup.

        Parameters
        ----------
        tier: str
            Name of the tier to update.
        id: str
            id of the lookup to create or update
        defn: obj | dict
            Definition object as a Python object which is converted to
            JSON internally.
        
        Reference
        ---------
        `POST /druid/coordinator/v1/lookups/config/{tier}/{id}`

        See https://druid.apache.org/docs/latest/querying/lookups.html
        """
        return self.post_json(REQ_LU_TIER_CONFIG, defn, args=[tier])
    
    #-------- Cluster membership --------

    def cluster_nodes(self, full=True):
        """
        Returns a list of services known to the cluster.

        Reference
        ---------
        `GET /druid/coordinator/v1/cluster?full`

        Currently undocumented.
        """
        return self.get_json(REQ_CLUSTER, params={"full": ""})


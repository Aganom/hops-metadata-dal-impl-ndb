/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.AceDataAccess;
import io.hops.metadata.hdfs.entity.Ace;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.NdbBoolean;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AceClusterJ implements TablesDef.AcesTableDef, AceDataAccess<Ace> {
  private ClusterjConnector connector = ClusterjConnector.getInstance();
  
  @PersistenceCapable(table = TABLE_NAME)
  public interface AceDto {
    
    @PrimaryKey
    @Column(name = ID)
    int getId();
    void setId(int id);
    
    @Column(name = INODE_ID)
    int getInodeId();
    void setInodeId(int inodeId);
    
    @Column(name = SUBJECT)
    String getSubject();
    void setSubject(String subject);
  
    @Column(name = TYPE)
    int getType();
    void setType(int type);
    
    @Column(name = IS_DEFAULT)
    byte getIsDefault();
    void setIsDefault(byte isDefault);
    
    @Column(name = PERMISSION)
    int getPermission();
    void setPermission(int permission);
    
    @Column(name = INDEX)
    int getIndex();
    void setIndex(int index);
  }
  
  @Override
  public Ace addAce(Ace toAdd)
      throws StorageException {
    HopsSession session = connector.obtainSession();
  
    AceDto persistable = session.newInstance(AceDto.class);
    persistable.setPermission(toAdd.getPermission());
    persistable.setIsDefault(NdbBoolean.convert(toAdd.isDefault()));
    persistable.setInodeId(toAdd.getInodeId());
    persistable.setSubject(toAdd.getSubject());
    persistable.setType(toAdd.getType().getValue());
    persistable.setIndex(toAdd.getIndex());
    
    AceDto inserted = session.makePersistent(persistable);
    
    Ace toReturn = fromDto(inserted);
    session.release(inserted);
    return toReturn;
  }
  
  @Override
  public List<Ace> getAcesByInodeId(int inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<AceDto> queryResults;
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<AceDto> dobj =
        qb.createQueryDefinition(AceDto.class);
    HopsPredicate pred1 = dobj.get("inodeId").equal(dobj.param("inodeIdParam"));
    dobj.where(pred1);
    HopsQuery<AceDto> query = session.createQuery(dobj);
    query.setParameter("inodeIdParam", inodeId);

    queryResults = query.getResultList();
    List<Ace> toReturn = new ArrayList<>();
    for (AceDto result : queryResults){
      toReturn.add(fromDto(result));
      session.release(result);
    }
    return toReturn;
  
  }
  
  @Override
  public void removeAcesForInodeId(int inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<AceDto> toDelete = new ArrayList<>();
    List<Ace> aces = getAcesByInodeId(inodeId);
    for (Ace ace : aces) {
      AceDto aceDto = createPersistable(session, ace);
      toDelete.add(aceDto);
    }
    session.deletePersistentAll(toDelete);
    session.release(toDelete);
  }
  
  @Override
  public Ace getAceByPK(int id, int inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    Object[] pk = new Object[2];
    pk[0] = id;
    pk[1] = inodeId;
  
    AceDto result = session.find(AceDto.class, pk);
    if (result != null) {
      Ace ace = fromDto(result);
      session.release(result);
      return ace;
    } else {
      return null;
    }
  }
  
  @Override
  public List<Ace> getAcesByPKBatched(int[] ids, int inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
  
    List<AceDto> dtos = new ArrayList<>();
    try {
      for (int i = 0; i < ids.length; i++) {
        AceDto dto = session.newInstance(AceDto.class);
        dto.setId(ids[i]);
        dto = session.load(dto);
        dtos.add(dto);
      }
      session.flush();
      List<Ace> aces = fromDtos(dtos);
      return aces;
    } finally {
      session.release(dtos);
    }
  }
  
  @Override
  public void prepare(Collection<Ace> removed, Collection<Ace> modified) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<AceDto> changes = new ArrayList<>();
    List<AceDto> deletions = new ArrayList<>();
    try {
      for (Ace ace : removed) {
        Object[] pk = new Object[2];
        pk[0] = ace.getInodeId();
        pk[2] = ace.getId();
        AceDto persistable = session.newInstance(AceDto.class, pk);
        deletions.add(persistable);
      }
      
      for (Ace ace : modified) {
        AceDto persistable = createPersistable(session, ace);
        changes.add(persistable);
      }
      session.deletePersistentAll(deletions);
      session.savePersistentAll(changes);
    } finally {
      session.release(deletions);
      session.release(changes);
    }
  }
  
  private List<Ace> fromDtos(List<AceDto> dtos){
    ArrayList<Ace> toReturn = new ArrayList<>();
    for (AceDto dto : dtos) {
      toReturn.add(fromDto(dto));
    }
    return toReturn;
  }
  
  private Ace fromDto(AceDto dto){
    int id = dto.getId();
    int inode = dto.getInodeId();
    String subject = dto.getSubject();
    boolean isDefault = NdbBoolean.convert(dto.getIsDefault());
    int permission = dto.getPermission();
    Ace.AceType type = Ace.AceType.valueOf(dto.getType());
    int index = dto.getIndex();
    
    return new Ace(id, inode, subject, type, isDefault, permission, index);
  }
  
  private AceDto createPersistable(HopsSession session, Ace from) throws StorageException {
    AceDto aceDto = session.newInstance(AceDto.class);
    aceDto.setId(from.getId());
    aceDto.setInodeId(from.getInodeId());
    aceDto.setSubject(from.getSubject());
    aceDto.setIsDefault(NdbBoolean.convert(from.isDefault()));
    aceDto.setPermission(from.getPermission());
    aceDto.setType(from.getType().getValue());
    aceDto.setIndex(from.getIndex());
    return aceDto;
  }
}

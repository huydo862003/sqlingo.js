import {
  describe, expect, it,
} from 'vitest';
import {
  sqlToDbml,
} from '../index';
import {
  DbmlReferenceAction, DbmlRelation,
} from '../types';

describe('refs', () => {
  it('captures inline REFERENCES with actions', () => {
    const {
      schema,
    } = sqlToDbml(`
      CREATE TABLE orders (
        id INT,
        user_id INT REFERENCES users(id) ON DELETE CASCADE ON UPDATE SET NULL
      );
    `, 'postgres');
    expect(schema.refs).toHaveLength(1);
    expect(schema.refs[0]).toMatchObject({
      relation: DbmlRelation.MANY_TO_ONE,
      source: {
        table: 'orders',
        columns: ['user_id'],
      },
      target: {
        table: 'users',
        columns: ['id'],
      },
      onDelete: DbmlReferenceAction.CASCADE,
      onUpdate: DbmlReferenceAction.SET_NULL,
    });
  });

  it('captures named CONSTRAINT FOREIGN KEY wrapper', () => {
    const {
      schema,
    } = sqlToDbml(
      'CREATE TABLE a (id INT, b_id INT, CONSTRAINT fk_ab FOREIGN KEY (b_id) REFERENCES b(id) ON DELETE CASCADE);',
      'postgres',
    );
    expect(schema.refs).toHaveLength(1);
    expect(schema.refs[0]).toMatchObject({
      name: 'fk_ab',
      source: {
        columns: ['b_id'],
      },
      target: {
        table: 'b',
        columns: ['id'],
      },
      onDelete: DbmlReferenceAction.CASCADE,
    });
  });

  it('captures table-level FOREIGN KEY with actions', () => {
    const {
      schema,
    } = sqlToDbml(
      'CREATE TABLE a (id INT, b_id INT, FOREIGN KEY (b_id) REFERENCES b(id) ON DELETE RESTRICT);',
      'postgres',
    );
    expect(schema.refs[0].onDelete).toBe(DbmlReferenceAction.RESTRICT);
    expect(schema.refs[0].source.columns).toEqual(['b_id']);
  });
});

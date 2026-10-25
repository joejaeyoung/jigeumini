import { expect, it } from 'vitest';
import { parseSaved } from './useSavedChannels';
it('손상·다른 형식의 저장값은 빈 목록으로 복구한다', () => {
  for (const value of [null, '{', '{}', 'null', '1']) expect(parseSaved(value)).toEqual([]);
});
it('실제 형식의 채널 ID만 중복 없이 복원한다', () => {
  const id = 'a'.repeat(32);
  expect(parseSaved(JSON.stringify([id, 'sand', id, 123, '<script>']))).toEqual([id]);
});
